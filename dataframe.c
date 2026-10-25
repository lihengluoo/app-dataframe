/*
 * Numeric C port of Polars' group-index and aggregation kernels, rs-0.51.0.
 *
 * Upstream, https://github.com/pola-rs/polars/tree/rs-0.51.0:
 *   crates/polars-core/src/frame/group_by/hashing.rs: group_by_threaded_slice/iter
 *   crates/polars-core/src/frame/group_by/position.rs: GroupsIdx
 *   crates/polars-core/src/frame/group_by/aggregations/mod.rs: agg_sum/min/max/mean
 */
#include "dataframe.h"

#include <fcntl.h>
#include <float.h>
#include <inttypes.h>
#include <math.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

pthread_barrier_t worker_barrier;
/* Main sets this once before create. Workers never modify the chain head. */
const worker_input *worker_inputs;

/* The only clock interface; the DSM rewriter can replace this function.
 * Cross-node execution needs a common clock basis for worker timestamps. */
double elapsed_ms(double begin_ms)
{
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (double)now.tv_sec * 1000.0 + (double)now.tv_nsec / 1000000.0 -
           begin_ms;
}

int text_equal(const char *left, const char *right)
{
    size_t index = 0;
    while (left[index] && left[index] == right[index]) {
        ++index;
    }
    return left[index] == right[index];
}

int copy_argument(input_path *dst, const char *src)
{
    size_t index = 0;
    while (index + 1 < sizeof(dst->bytes) && src[index]) {
        dst->bytes[index] = src[index];
        ++index;
    }
    dst->bytes[index] = '\0';
    return src[index] != '\0';
}

int parse_count(const char *digits, size_t *count)
{
    size_t parsed = 0, index = 0;
    while (digits[index] >= '0' && digits[index] <= '9') {
        unsigned digit = (unsigned)(digits[index] - '0');
        if (parsed > (SIZE_MAX - digit) / 10) {
            return -1;
        }
        parsed = parsed * 10 + digit;
        ++index;
    }
    if (!index || digits[index] || !parsed) {
        return -1;
    }
    *count = parsed;
    return 0;
}

/* -1 is the requested range's end; -2 is an I/O error or a truncated range. */
int read_csv_byte(csv_reader *reader)
{
    if (reader->cursor == reader->count) {
        size_t bytes = sizeof(reader->bytes);
        if (reader->end >= 0) {
            off_t remaining = reader->end - reader->position;
            if (remaining <= 0) {
                return -1;
            }
            if (remaining < (off_t)bytes) {
                bytes = (size_t)remaining;
            }
        }
        ssize_t count =
            pread(reader->fd, reader->bytes, bytes, reader->position);
        if (count < 0 || (count == 0 && reader->end >= 0)) {
            return -2;
        }
        if (count == 0) {
            return -1;
        }
        reader->cursor = 0;
        reader->count = (size_t)count;
    }
    ++reader->position;
    return reader->bytes[reader->cursor++];
}

/* LF and CRLF, blank lines and an unterminated final row are supported.
 * The fixed schema has no quoted fields or embedded newlines. */
int skip_csv_row(csv_reader *reader)
{
    int occupied = 0;
    for (;;) {
        int byte = read_csv_byte(reader);
        if (byte == -2) {
            return -1;
        }
        if (byte == -1) {
            return occupied;
        }
        if (byte == '\n') {
            if (occupied) {
                return 1;
            }
        } else if (byte != '\r') {
            occupied = 1;
        }
    }
}

/* Main reads the header and finds EOF with logarithmically many one-byte
 * preads. It never scans the full file or counts rows. No lseek/fstat API
 * is needed; the regular input file must remain unchanged during a run. */
int read_csv_bounds(const input_path *path, off_t *data_begin, off_t *file_end)
{
    csv_reader reader = {.fd = open(path->bytes, O_RDONLY), .end = -1};
    if (reader.fd < 0) {
        return -1;
    }
    int byte, columns = 1;
    while ((byte = read_csv_byte(&reader)) >= 0 && byte != '\n') {
        columns += byte == ',';
    }
    if (byte != '\n' || columns != input_columns) {
        close(reader.fd);
        return -1;
    }
    *data_begin = reader.position;
    off_t lower = reader.position + (off_t)(reader.count - reader.cursor);
    off_t upper = lower;
    unsigned char byte_at_offset;
    int error = 0;
    for (;;) {
        ssize_t count = pread(reader.fd, &byte_at_offset, 1, upper);
        if (count <= 0) {
            error = count < 0;
            break;
        }
        if (upper > INT64_MAX / 2) {
            error = 1;
            break;
        }
        lower = upper + 1;
        upper *= 2;
    }
    while (!error && lower < upper) {
        off_t middle = lower + (upper - lower) / 2;
        ssize_t count = pread(reader.fd, &byte_at_offset, 1, middle);
        if (count < 0) {
            error = 1;
        } else if (count == 0) {
            upper = middle;
        } else {
            lower = middle + 1;
        }
    }
    *file_end = lower;
    close(reader.fd);
    return error ? -1 : 0;
}

/* The same byte boundary always rounds forward to the same row boundary.
 * Adjacent workers therefore neither omit nor duplicate a row, including
 * CRLF, empty ranges and an unterminated final row. All state is private. */
int align_csv_boundary(csv_reader *reader, off_t offset, off_t *aligned)
{
    reader->position = offset - 1;
    reader->cursor = reader->count = 0;
    int byte;
    while ((byte = read_csv_byte(reader)) >= 0 && byte != '\n') {
    }
    *aligned = reader->position;
    return byte == -2 ? error_input : error_none;
}

/* Numeric key IDs can have the H2O `id` prefix. Keys are UInt32, v1/v2 Int32;
 * v3 is finite double. Empty, NA and NULL fields are missing. */
int parse_csv_number(const char *text, size_t column, number *parsed,
                     int *valid)
{
    *parsed = (number){.bits = 0};
    *valid = 0;
    if (!text[0] || text_equal(text, "NA") || text_equal(text, "NULL") ||
        text_equal(text, "na") || text_equal(text, "null")) {
        return 0;
    }
    size_t index = 0;
    if (column < key_columns && text[0] == 'i' && text[1] == 'd') {
        index = 2;
    }
    int negative = text[index] == '-';
    if (text[index] == '-' || text[index] == '+') {
        ++index;
    }
    if (column < key_columns && negative) {
        return -1;
    }
    if (column != column_v3) {
        uint64_t magnitude = 0;
        uint64_t limit = column < key_columns ?
                             UINT32_MAX :
                             (uint64_t)INT32_MAX + (unsigned)negative;
        size_t first_digit = index;
        while (text[index] >= '0' && text[index] <= '9') {
            unsigned digit = (unsigned)(text[index] - '0');
            if (magnitude > (limit - digit) / 10) {
                return -1;
            }
            magnitude = magnitude * 10 + digit;
            ++index;
        }
        if (index == first_digit || text[index]) {
            return -1;
        }
        parsed->bits = negative ? UINT64_C(0) - magnitude : magnitude;
    } else {
        double real = 0.0, fraction = 1.0;
        size_t digits = 0;
        while (text[index] >= '0' && text[index] <= '9') {
            real = real * 10.0 + (text[index++] - '0');
            ++digits;
        }
        if (text[index] == '.') {
            ++index;
            while (text[index] >= '0' && text[index] <= '9') {
                fraction *= 0.1;
                real += (text[index++] - '0') * fraction;
                ++digits;
            }
        }
        if (!digits) {
            return -1;
        }
        if (text[index] || !(real <= DBL_MAX)) {
            return -1;
        }
        parsed->real = negative ? -real : real;
    }
    *valid = 1;
    return 0;
}

int read_csv_row(csv_reader *reader, number *cells, uint16_t *valid)
{
    char field[128];
    size_t used = 0, column = 0;
    *valid = 0;
    for (;;) {
        int byte = read_csv_byte(reader);
        if (byte == -2) {
            return -1;
        }
        if (byte == '\r') {
            continue;
        }
        if (byte == '\n' && column == 0 && used == 0) {
            continue;
        }
        if (byte == -1 && column == 0 && used == 0) {
            return 0;
        }
        if (byte == ',' || byte == '\n' || byte == -1) {
            if (column >= input_columns) {
                return -1;
            }
            field[used] = '\0';
            int present;
            if (parse_csv_number(field, column, &cells[column], &present)) {
                return -1;
            }
            *valid |= (uint16_t)((unsigned)present << column);
            ++column;
            used = 0;
            if (byte != ',') {
                return column == input_columns ? 1 : -1;
            }
        } else {
            if (used + 1 == sizeof(field)) {
                return -1;
            }
            field[used++] = (char)byte;
        }
    }
}

int load_input_shard(const worker_input *input, resident_frame *frame)
{
    csv_reader reader = {.fd = open(input->path->bytes, O_RDONLY),
                         .end = input->file_end};
    if (reader.fd < 0) {
        return error_input;
    }
    input_range range;
    int error = align_csv_boundary(&reader, input->range.begin, &range.begin);
    if (!error) {
        error = align_csv_boundary(&reader, input->range.end, &range.end);
    }
    if (!error) {
        reader.position = range.begin;
        reader.end = range.end;
        reader.cursor = reader.count = 0;
        int status;
        while ((status = skip_csv_row(&reader)) > 0) {
            ++frame->rows;
        }
        error = status < 0 ? error_input : error_none;
    }
    if (!error && frame->rows) {
        size_t row_bytes = key_columns * sizeof(uint32_t) +
                           2 * sizeof(int32_t) + sizeof(double);
        if (frame->rows >= (UINT64_C(1) << row_shard_shift) ||
            frame->rows > SIZE_MAX / row_bytes) {
            error = error_memory;
        } else {
            frame->keys = malloc(frame->rows * row_bytes);
            frame->valid = malloc(frame->rows * sizeof(*frame->valid));
            if (!frame->keys || !frame->valid) {
                error = error_memory;
            } else {
                frame->integers =
                    (int32_t *)(frame->keys + key_columns * frame->rows);
                frame->reals = (double *)(frame->integers + 2 * frame->rows);
            }
        }
    }
    /* Count and parse use the same private descriptor and exact row interval.
     * No scan or allocation in this function touches a peer's input. */
    if (!error) {
        reader.position = range.begin;
        reader.cursor = reader.count = 0;
    }
    for (size_t row = 0; !error && row < frame->rows; ++row) {
        number cells[input_columns];
        if (read_csv_row(&reader, cells, &frame->valid[row]) != 1) {
            error = error_input;
            break;
        }
        for (size_t column = 0; column < key_columns; ++column) {
            frame->keys[column * frame->rows + row] =
                (uint32_t)cells[column].bits;
        }
        frame->integers[row] = (int32_t)cells[column_v1].integer;
        frame->integers[frame->rows + row] = (int32_t)cells[column_v2].integer;
        frame->reals[row] = cells[column_v3].real;
    }
    close(reader.fd);
    return error;
}

size_t original_row_offset(size_t row_id)
{
    return row_id & ((UINT64_C(1) << row_shard_shift) - 1);
}

const resident_frame *original_shard(const group_input *input, size_t row_id)
{
    return &input->shards[row_id >> row_shard_shift];
}

number original_measure(const resident_frame *shard, size_t row,
                        column_id column)
{
    if (column == column_v3) {
        return (number){.real = shard->reals[row]};
    }
    return (number){
        .integer = shard->integers[(column - column_v1) * shard->rows + row]};
}

/* The C hash-table backend replaces hashbrown. Mix independently for bucket
 * selection rather than using the partition hash directly as a table index. */
uint64_t mix_hash(uint64_t hash)
{
    hash ^= hash >> 30;
    hash *= UINT64_C(0xbf58476d1ce4e5b9);
    hash ^= hash >> 27;
    hash *= UINT64_C(0x94d049bb133111eb);
    return hash ^ (hash >> 31);
}

/* Polars hash_to_partition takes the high half of hash * partitions.
 * Use 32-bit limbs so this remains C11 without a compiler-specific type. */
size_t key_partition(uint64_t hash, size_t partitions)
{
    uint64_t count = partitions;
    uint64_t low = (uint64_t)(uint32_t)hash * (uint32_t)count;
    uint64_t middle = (hash >> 32) * (uint32_t)count + (low >> 32);
    uint64_t upper = (uint64_t)(uint32_t)hash * (count >> 32) +
                     (uint32_t)middle;
    return (size_t)((hash >> 32) * (count >> 32) + (middle >> 32) +
                    (upper >> 32));
}

uint64_t hash_numeric_key(const resident_frame *frame, size_t row,
                          column_id column)
{
    return frame->valid[row] & (1u << column) ?
               (uint64_t)frame->keys[column * frame->rows + row] *
                   numeric_hash_multiplier :
               0;
}

uint64_t hash_key_bytes(const unsigned char *bytes, size_t count)
{
    uint64_t hash = encoded_hash_basis;
    for (size_t index = 0; index < count; ++index) {
        hash = (hash ^ bytes[index]) * encoded_hash_multiplier;
    }
    return mix_hash(hash);
}

/* Numeric subset of Polars' unordered row encoding: one validity byte and
 * four big-endian bytes per UInt32 column, in original input row order. */
int encode_group_keys(const resident_frame *frame, const query_spec *query,
                      encoded_keys *keys)
{
    size_t row_bytes = query->key_count * encoded_column_bytes;
    if (frame->rows == 0) {
        return error_none;
    }
    if (frame->rows > SIZE_MAX / row_bytes ||
        frame->rows > SIZE_MAX / sizeof(uint64_t)) {
        return error_memory;
    }
    keys->bytes = malloc(frame->rows * row_bytes);
    keys->hashes = malloc(frame->rows * sizeof(*keys->hashes));
    if (!keys->bytes || !keys->hashes) {
        return error_memory;
    }
    for (size_t key = 0; key < query->key_count; ++key) {
        size_t column = query->keys[key];
        for (size_t row = 0; row < frame->rows; ++row) {
            unsigned char *bytes = keys->bytes + row * row_bytes +
                                   key * encoded_column_bytes;
            unsigned present = (frame->valid[row] >> column) & 1u;
            uint32_t cell = present ? frame->keys[column * frame->rows + row] : 0;
            bytes[0] = (unsigned char)present;
            for (size_t byte = 0; byte < encoded_uint32_bytes; ++byte) {
                size_t shift = 8 * (encoded_uint32_bytes - 1 - byte);
                bytes[1 + byte] = (unsigned char)(cell >> shift);
            }
        }
    }
    for (size_t row = 0; row < frame->rows; ++row) {
        keys->hashes[row] = hash_key_bytes(keys->bytes + row * row_bytes, row_bytes);
    }
    return error_none;
}

int group_keys_equal(const group_input *input, size_t left_id, size_t right_id)
{
    if (input->query->key_count > 1) {
        size_t row_bytes = input->query->key_count * encoded_column_bytes;
        const unsigned char *left =
            input->keys[left_id >> row_shard_shift].bytes +
            original_row_offset(left_id) * row_bytes;
        const unsigned char *right =
            input->keys[right_id >> row_shard_shift].bytes +
            original_row_offset(right_id) * row_bytes;
        return memcmp(left, right, row_bytes) == 0;
    }
    const resident_frame *left = original_shard(input, left_id);
    const resident_frame *right = original_shard(input, right_id);
    size_t left_row = original_row_offset(left_id);
    size_t right_row = original_row_offset(right_id);
    for (size_t key = 0; key < input->query->key_count; ++key) {
        size_t column = input->query->keys[key];
        unsigned left_valid = (left->valid[left_row] >> column) & 1u;
        unsigned right_valid = (right->valid[right_row] >> column) & 1u;
        if (left_valid != right_valid ||
            (left_valid && left->keys[column * left->rows + left_row] !=
                               right->keys[column * right->rows + right_row])) {
            return 0;
        }
    }
    return 1;
}

int resize_group_hash(group_indices *groups, size_t capacity)
{
    if (capacity > SIZE_MAX / sizeof(hash_slot)) {
        return error_memory;
    }
    hash_slot *slots = malloc(capacity * sizeof(*slots));
    if (!slots) {
        return error_memory;
    }
    for (size_t index = 0; index < capacity; ++index) {
        slots[index].group = SIZE_MAX;
    }
    for (size_t index = 0; index < groups->slot_capacity; ++index) {
        const hash_slot *previous = &groups->slots[index];
        if (previous->group != SIZE_MAX) {
            size_t slot = (size_t)mix_hash(previous->hash) & (capacity - 1);
            while (slots[slot].group != SIZE_MAX) {
                slot = (slot + 1) & (capacity - 1);
            }
            slots[slot] = *previous;
        }
    }
    free(groups->slots);
    groups->slots = slots;
    groups->slot_capacity = capacity;
    return error_none;
}

int append_group_row(group_rows *group, size_t row_id)
{
    if (group->count == 0) {
        group->first = row_id;
    } else {
        if (group->count >= group->row_capacity) {
            size_t capacity = group->row_capacity ? group->row_capacity * 2 : 2;
            if (capacity < group->row_capacity ||
                capacity > SIZE_MAX / sizeof(size_t)) {
                return error_memory;
            }
            size_t *row_ids = malloc(capacity * sizeof(*row_ids));
            if (!row_ids) {
                return error_memory;
            }
            row_ids[0] = group->first;
            for (size_t index = 1; index < group->count; ++index) {
                row_ids[index] = group->row_ids[index];
            }
            free(group->row_ids);
            group->row_ids = row_ids;
            group->row_capacity = capacity;
        }
        group->row_ids[group->count] = row_id;
    }
    ++group->count;
    return error_none;
}

/* Like hashing.rs, append original row IDs directly to the matching group.
 * Numeric collisions compare typed cells; composite collisions compare the
 * prepared row bytes rather than copying another tuple into each group. */
int index_group_row(const group_input *input, group_indices *groups,
                    size_t row_id, uint64_t hash)
{
    if (groups->slot_capacity == 0 &&
        resize_group_hash(groups, initial_group_capacity)) {
        return error_memory;
    }
    size_t slot = (size_t)mix_hash(hash) & (groups->slot_capacity - 1);
    while (groups->slots[slot].group != SIZE_MAX) {
        size_t group = groups->slots[slot].group;
        if (groups->slots[slot].hash == hash &&
            group_keys_equal(input, row_id, groups->groups[group].first)) {
            break;
        }
        slot = (slot + 1) & (groups->slot_capacity - 1);
    }
    if (groups->slots[slot].group == SIZE_MAX) {
        if (groups->group_count + 1 >
            groups->slot_capacity - groups->slot_capacity / 4) {
            if (groups->slot_capacity > SIZE_MAX / 2 ||
                resize_group_hash(groups, groups->slot_capacity * 2)) {
                return error_memory;
            }
            slot = (size_t)mix_hash(hash) & (groups->slot_capacity - 1);
            while (groups->slots[slot].group != SIZE_MAX) {
                slot = (slot + 1) & (groups->slot_capacity - 1);
            }
        }
        if (groups->group_count == groups->group_capacity) {
            size_t capacity = groups->group_capacity ?
                                  groups->group_capacity * 2 :
                                  initial_group_capacity;
            if (capacity < groups->group_capacity ||
                capacity > SIZE_MAX / sizeof(group_rows)) {
                return error_memory;
            }
            group_rows *rows = malloc(capacity * sizeof(*rows));
            if (!rows) {
                return error_memory;
            }
            for (size_t index = 0; index < groups->group_count; ++index) {
                rows[index] = groups->groups[index];
            }
            free(groups->groups);
            groups->groups = rows;
            groups->group_capacity = capacity;
        }
        size_t group = groups->group_count++;
        groups->groups[group] = (group_rows){0};
        groups->slots[slot] = (hash_slot){.hash = hash, .group = group};
    }
    return append_group_row(&groups->groups[groups->slots[slot].group], row_id);
}

void free_group_indices(group_indices *groups)
{
    for (size_t group = 0; group < groups->group_count; ++group) {
        free(groups->groups[group].row_ids);
    }
    free(groups->slots);
    free(groups->groups);
}

/* Each reduction traverses its original row IDs independently. Count(*)
 * uses the complete group's length; count(column) reads only validity.
 * The benchmark keeps DRust's Float64 interface for numeric reductions. */
number aggregate_group(const group_input *input, const group_indices *groups,
                       size_t group, const aggregate_spec *agg, int *valid)
{
    const group_rows *rows = &groups->groups[group];
    if (agg->type == aggregate_count_all) {
        *valid = 1;
        return (number){.bits = rows->count};
    }
    size_t count = 0;
    double accumulated = 0.0;
    for (size_t offset = 0; offset < rows->count; ++offset) {
        size_t row_id = rows->count == 1 ? rows->first : rows->row_ids[offset];
        const resident_frame *shard = original_shard(input, row_id);
        size_t row = original_row_offset(row_id);
        if (!(shard->valid[row] & (1u << agg->column))) {
            continue;
        }
        if (agg->type == aggregate_count_nonnull) {
            ++count;
            continue;
        }
        number cell = original_measure(shard, row, agg->column);
        double numeric = agg->column == column_v3 ? cell.real :
                                                    (double)cell.integer;
        if (agg->type == aggregate_sum || agg->type == aggregate_mean) {
            accumulated += numeric;
        } else if (!count ||
                   (agg->type == aggregate_min && numeric < accumulated) ||
                   (agg->type == aggregate_max && numeric > accumulated)) {
            accumulated = numeric;
        }
        ++count;
    }
    *valid = count != 0 || agg->type == aggregate_sum ||
             agg->type == aggregate_count_nonnull;
    if (agg->type == aggregate_count_nonnull) {
        return (number){.bits = count};
    }
    if (agg->type == aggregate_mean && count) {
        accumulated /= (double)count;
    }
    return (number){.real = accumulated};
}

/* Full columnar output for one worker's slice of the completed group index.
 * Numeric columns are doubles (except counts); keys keep UInt32. Results
 * stay alive through the last query under the benchmark driver policy. */
result_frame *append_result(query_result *result, const query_spec *query,
                            size_t groups)
{
    size_t row_bytes = query->aggregate_count * sizeof(number) +
                       query->key_count * sizeof(uint32_t) + sizeof(uint16_t);
    if (groups > (SIZE_MAX - sizeof(result_frame)) / row_bytes) {
        return NULL;
    }
    result_frame *frame = malloc(sizeof(*frame) + groups * row_bytes);
    if (!frame) {
        return NULL;
    }
    *frame = (result_frame){.groups = groups};
    frame->aggregates = (number *)(frame + 1);
    frame->keys =
        (uint32_t *)(frame->aggregates + groups * query->aggregate_count);
    frame->valid = (uint16_t *)(frame->keys + groups * query->key_count);
    if (result->last) {
        result->last->next = frame;
    } else {
        result->first = frame;
    }
    result->last = frame;
    result->groups += groups;
    return frame;
}

/* Appends one formatted field and returns the new end offset, stopping at
 * the line capacity. */
size_t append_field(char *dst, size_t capacity, size_t used,
                    const char *format, ...)
{
    va_list args;
    va_start(args, format);
    int written = vsnprintf(dst + used, capacity - used, format, args);
    va_end(args);
    if (written < 0 || (size_t)written >= capacity - used) {
        return capacity - 1;
    }
    return used + (size_t)written;
}

/* The worker formats its own preview lines; main prints finished text and
 * never decodes keys or aggregates. Output matches the printed row format. */
void format_result_row(char *dst, size_t capacity, const result_frame *frame,
                       const query_spec *query, size_t group)
{
    size_t used = 0;
    for (size_t key = 0; key < query->key_count; ++key) {
        if (key) {
            dst[used++] = ',';
        }
        if (frame->valid[group] & (1u << key)) {
            used = append_field(dst, capacity, used, "%" PRIu32,
                                frame->keys[key * frame->groups + group]);
        } else {
            used = append_field(dst, capacity, used, "NULL");
        }
    }
    for (size_t index = 0; index < query->aggregate_count; ++index) {
        dst[used++] = ',';
        if (!(frame->valid[group] & (1u << (8 + index)))) {
            used = append_field(dst, capacity, used, "NULL");
            continue;
        }
        const aggregate_spec *agg = &query->aggregates[index];
        number cell = frame->aggregates[index * frame->groups + group];
        if (agg->type == aggregate_count_nonnull ||
            agg->type == aggregate_count_all) {
            used = append_field(dst, capacity, used, "%" PRIu64, cell.bits);
        } else {
            used = append_field(dst, capacity, used, "%.17g", cell.real);
        }
    }
    if (used + 1 < capacity) {
        dst[used++] = '\n';
    }
    dst[used] = '\0';
}

/* DRust GroupBy::keys / take: use representative ORIGINAL row IDs to
 * materialize every output key column, independently of aggregate columns. */
void materialize_keys(const group_input *input, const group_indices *groups,
                      result_frame *result)
{
    for (size_t group = 0; group < groups->group_count; ++group) {
        result->valid[group] = 0;
    }
    for (size_t key = 0; key < input->query->key_count; ++key) {
        size_t column = input->query->keys[key];
        for (size_t group = 0; group < groups->group_count; ++group) {
            size_t row_id = groups->groups[group].first;
            const resident_frame *shard = original_shard(input, row_id);
            size_t row = original_row_offset(row_id);
            result->keys[key * result->groups + group] =
                shard->keys[column * shard->rows + row];
            result->valid[group] |= ((shard->valid[row] >> column) & 1u) << key;
        }
    }
}

int materialize_groups(const group_input *input, const group_indices *groups,
                       query_result *result, query_preview *preview)
{
    result_frame *frame =
        append_result(result, input->query, groups->group_count);
    if (!frame) {
        return error_memory;
    }
    materialize_keys(input, groups, frame);
    for (size_t index = 0; index < input->query->aggregate_count; ++index) {
        const aggregate_spec *agg = &input->query->aggregates[index];
        /* No fusion: every operator constructs a complete result column. */
        for (size_t group = 0; group < groups->group_count; ++group) {
            int valid;
            number cell = aggregate_group(input, groups, group, agg, &valid);
            frame->valid[group] |= (uint16_t)valid << (8 + index);
            frame->aggregates[index * frame->groups + group] = cell;
        }
    }
    for (size_t group = 0;
         group < frame->groups && preview->rows < preview_rows; ++group) {
        format_result_row(preview->lines[preview->rows], preview_line_bytes,
                          frame, input->query, group);
        ++preview->rows;
    }
    preview->groups = result->groups;
    return error_none;
}

/* hashing.rs::group_by_threaded_slice/iter: every partition scans all key
 * slices and only inserts matching rows into its own new map. */
int group_partition(const group_input *input, size_t partition,
                    size_t partition_count, group_indices *groups)
{
    for (size_t shard = 0; shard < input->shard_count; ++shard) {
        const resident_frame *frame = &input->shards[shard];
        for (size_t row = 0; row < frame->rows; ++row) {
            uint64_t hash = input->query->key_count == 1 ?
                                hash_numeric_key(frame, row,
                                                 input->query->keys[0]) :
                                input->keys[shard].hashes[row];
            if (key_partition(hash, partition_count) == partition) {
                size_t row_id = (shard << row_shard_shift) | row;
                int error = index_group_row(input, groups, row_id, hash);
                if (error) {
                    return error;
                }
            }
        }
    }
    free(groups->slots);
    groups->slots = NULL;
    groups->slot_capacity = 0;
    return error_none;
}

int build_partition_groups(worker_input *worker, const group_input *input,
                           size_t partition_count)
{
    size_t count = worker->worker_id < partition_count ?
                       1 + (partition_count - 1 - worker->worker_id) /
                               worker->thread_count :
                       0;
    worker->partitions = NULL;
    worker->group_partition_count = 0;
    if (count == 0) {
        return error_none;
    }
    if (count > SIZE_MAX / sizeof(group_indices)) {
        return error_memory;
    }
    worker->partitions = malloc(count * sizeof(*worker->partitions));
    if (!worker->partitions) {
        return error_memory;
    }
    worker->group_partition_count = count;
    for (size_t index = 0; index < count; ++index) {
        worker->partitions[index] = (group_indices){0};
    }
    for (size_t index = 0; index < count; ++index) {
        size_t partition = worker->worker_id + index * worker->thread_count;
        int error = group_partition(input, partition, partition_count,
                                    &worker->partitions[index]);
        if (error) {
            return error;
        }
    }
    return error_none;
}

/* finish_group_order collects complete groups before any reduction. Each
 * worker takes a disjoint slice of the unsorted collection, independently
 * of which partition task built a group. Row lists remain shared read-only. */
int collect_group_slice(const worker_input *worker, group_indices *groups)
{
    size_t total = 0;
    for (const worker_input *peer = worker_inputs; peer; peer = peer->next) {
        if (peer->query_error) {
            return error_peer;
        }
        for (size_t index = 0; index < peer->group_partition_count; ++index) {
            total += peer->partitions[index].group_count;
        }
    }
    size_t per_worker = total / worker->thread_count;
    size_t extra = total % worker->thread_count;
    size_t begin = worker->worker_id * per_worker +
                   (worker->worker_id < extra ? worker->worker_id : extra);
    size_t count = per_worker + (worker->worker_id < extra);
    if (count == 0) {
        return error_none;
    }
    if (count > SIZE_MAX / sizeof(group_rows)) {
        return error_memory;
    }
    groups->groups = malloc(count * sizeof(*groups->groups));
    if (!groups->groups) {
        return error_memory;
    }
    size_t offset = 0;
    for (const worker_input *peer = worker_inputs; peer; peer = peer->next) {
        for (size_t index = 0; index < peer->group_partition_count; ++index) {
            const group_indices *partition = &peer->partitions[index];
            size_t first = begin > offset ? begin - offset : 0;
            for (size_t group = first;
                 group < partition->group_count && groups->group_count < count;
                 ++group) {
                groups->groups[groups->group_count++] = partition->groups[group];
            }
            offset += partition->group_count;
        }
    }
    return error_none;
}

void free_partition_groups(worker_input *worker)
{
    for (size_t index = 0; index < worker->group_partition_count; ++index) {
        free_group_indices(&worker->partitions[index]);
    }
    free(worker->partitions);
    worker->partitions = NULL;
    worker->group_partition_count = 0;
}

void free_query_results(query_result *results, size_t query_count)
{
    if (!results) {
        return;
    }
    for (size_t query = 0; query < query_count; ++query) {
        result_frame *block = results[query].first;
        while (block) {
            result_frame *next = block->next;
            free(block);
            block = next;
        }
    }
    free(results);
}

int read_input_shards(group_input *input)
{
    for (const worker_input *peer = worker_inputs; peer; peer = peer->next) {
        if (peer->error) {
            return error_peer;
        }
        if (peer->frame.rows > SIZE_MAX - input->rows) {
            return error_memory;
        }
        input->shards[peer->worker_id] = peer->frame;
        input->rows += peer->frame.rows;
    }
    return error_none;
}

int read_encoded_keys(group_input *input)
{
    for (const worker_input *peer = worker_inputs; peer; peer = peer->next) {
        if (peer->keys.error) {
            return error_peer;
        }
        input->keys[peer->worker_id] = peer->keys;
    }
    return error_none;
}

/* Barriers implement actual stage dependencies: publish immutable inputs,
 * publish composite-key encoding when needed, complete grouping, then finish
 * reductions before releasing shared row lists. Numeric keys have no producer
 * phase. Errors suppress work, never the waits required by the query. */
void *worker_entry(void *argument)
{
    worker_input *input = argument;
    worker_report *report =
        malloc(sizeof(*report) + input->query_count * sizeof(query_preview));
    query_result *results = malloc(input->query_count * sizeof(*results));
    int error = error_none;
    if (!report || !results) {
        error = error_memory;
    }
    if (report) {
        *report = (worker_report){0};
        for (size_t query = 0; query < input->query_count; ++query) {
            report->queries[query] = (query_preview){0};
        }
    }
    if (results) {
        for (size_t query = 0; query < input->query_count; ++query) {
            results[query] = (query_result){0};
        }
    }
    group_input received = {
        .shards = malloc(input->thread_count * sizeof(resident_frame)),
        .keys = malloc(input->thread_count * sizeof(encoded_keys)),
        .shard_count = input->thread_count};
    if (!received.shards || !received.keys) {
        error = error_memory;
    }
    if (!error) {
        error = load_input_shard(input, &input->frame);
    }
    input->error = error;

    pthread_barrier_wait(&worker_barrier); /* Original columns are published. */
    double query_begin_ms = elapsed_ms(0.0);
    if (!error) {
        error = read_input_shards(&received);
    }
    for (size_t query_index = 0; query_index < input->query_count;
         ++query_index) {
        size_t op = input->schedule[query_index % input->schedule_count];
        const query_spec *query = &input->queries[op];
        received.query = query;
        if (query->key_count > 1) {
            input->keys = (encoded_keys){.error = error};
            if (!error) {
                input->keys.error = encode_group_keys(&input->frame, query,
                                                      &input->keys);
                error = input->keys.error;
            }
            pthread_barrier_wait(&worker_barrier); /* Encoded keys are ready. */
            if (!error) {
                error = read_encoded_keys(&received);
            }
        }
        size_t partition_count = input->partition_count;
        if (input->thread_count == 1 ||
            (query->key_count == 1 &&
             received.rows <= numeric_thread_min_rows)) {
            partition_count = 1;
        }
        if (!error) {
            error = build_partition_groups(input, &received, partition_count);
        }
        input->query_error = error;
        pthread_barrier_wait(&worker_barrier); /* Every group is complete. */
        if (query->key_count > 1) {
            free(input->keys.bytes);
            free(input->keys.hashes);
        }
        if (!error) {
            group_indices groups = {0};
            error = collect_group_slice(input, &groups);
            if (!error) {
                error = materialize_groups(&received, &groups,
                                           &results[query_index],
                                           &report->queries[query_index]);
            }
            free(groups.groups);
        }
        pthread_barrier_wait(&worker_barrier); /* All group readers are done. */
        free_partition_groups(input);
    }
    free(received.keys);
    free(received.shards);
    double query_end_ms = elapsed_ms(0.0);

    /* Keeping completed results is the benchmark driver's lifetime policy. */
    free(input->frame.keys);
    free(input->frame.valid);
    free_query_results(results, input->query_count);
    if (report) {
        report->error = error;
        report->input_rows = input->frame.rows;
        report->query_begin_ms = query_begin_ms;
        report->query_end_ms = query_end_ms;
    }
    return report;
}

/* O(workers + preview_rows) per query; there is no main-thread full-result
 * scan, checksum, global sort, or summation of already complete groups. */
void print_query_results(const query_spec *queries, const size_t *schedule,
                         size_t schedule_count, size_t query_count,
                         const worker_report *reports)
{
    for (size_t index = 0; index < query_count; ++index) {
        const query_spec *query = &queries[schedule[index % schedule_count]];
        size_t groups = 0;
        for (const worker_report *report = reports; report;
             report = report->next) {
            groups += report->queries[index].groups;
        }
        printf("query=%zu op=Q%zu groups=%zu\n", index + 1, query->id, groups);
        for (size_t key = 0; key < query->key_count; ++key) {
            printf("%s%s", key ? "," : "", column_names[query->keys[key]]);
        }
        for (size_t agg = 0; agg < query->aggregate_count; ++agg) {
            const aggregate_spec *spec = &query->aggregates[agg];
            if (spec->type == aggregate_count_all) {
                printf(",count(*)");
            } else {
                printf(",%s(%s)", aggregate_names[spec->type],
                       column_names[spec->column]);
            }
        }
        printf("\n");
        size_t printed = 0;
        for (const worker_report *report = reports;
             report && printed < preview_rows; report = report->next) {
            const query_preview *preview = &report->queries[index];
            for (size_t row = 0; row < preview->rows && printed < preview_rows;
                 ++row) {
                fputs(preview->lines[row], stdout);
                ++printed;
            }
        }
    }
}

void free_worker_reports(worker_report *report)
{
    while (report) {
        worker_report *next_report = report->next;
        free(report);
        report = next_report;
    }
}

void free_worker_inputs(worker_input *input)
{
    while (input) {
        worker_input *next = input->next;
        free(input);
        input = next;
    }
}

void print_usage(void)
{
    printf(
        "Usage: dataframe [--threads N] [--partitions P] [--passes R]\n"
        "                 [--suite drust|extended] [--query Q] input.csv\n"
        "H2O columns: id1,id2,id3,id4,id5,id6,v1,v2,v3; header required.\n"
        "Defaults: 8 workers, P=N, 1 pass, DRust's 29 query occurrences.\n"
        "Extended: Q1..Q13 once. --query Q: 1..13.\n");
}

/* Return 0 for valid arguments, 1 for help, and -1 for invalid input. */
int parse_arguments(int argc, char **argv, benchmark_config *config)
{
    for (int index = 1; index < argc; ++index) {
        input_path argument;
        if (copy_argument(&argument, argv[index])) {
            return -1;
        }
        if (text_equal(argument.bytes, "--help")) {
            return 1;
        }
        if (argument.bytes[0] == '-') {
            if (index + 1 == argc) {
                return -1;
            }
            input_path parameter;
            if (copy_argument(&parameter, argv[++index])) {
                return -1;
            }
            int error;
            if (text_equal(argument.bytes, "--threads")) {
                error = parse_count(parameter.bytes, &config->thread_count);
            } else if (text_equal(argument.bytes, "--partitions")) {
                error = parse_count(parameter.bytes, &config->partition_count);
            } else if (text_equal(argument.bytes, "--passes")) {
                error = parse_count(parameter.bytes, &config->passes);
            } else if (text_equal(argument.bytes, "--query")) {
                error = parse_count(parameter.bytes, &config->selected_query);
            } else if (text_equal(argument.bytes, "--suite")) {
                error = 0;
                if (text_equal(parameter.bytes, "drust")) {
                    config->suite = suite_drust;
                } else if (text_equal(parameter.bytes, "extended")) {
                    config->suite = suite_extended;
                } else {
                    error = -1;
                }
            } else {
                error = -1;
            }
            if (error) {
                return -1;
            }
        } else if (!config->path.bytes[0]) {
            config->path = argument;
        } else {
            return -1;
        }
    }
    if (config->path.bytes[0] && config->thread_count <= max_worker_threads &&
        config->selected_query <= query_spec_count) {
        if (config->partition_count == 0) {
            config->partition_count = config->thread_count;
        }
        return 0;
    }
    return -1;
}

int prepare_query_schedule(const benchmark_config *config,
                           query_schedule *schedule)
{
    if (config->selected_query) {
        schedule->indices[schedule->count++] = config->selected_query - 1;
    } else if (config->suite == suite_extended) {
        for (size_t query = 0; query < extended_query_count; ++query) {
            schedule->indices[schedule->count++] = query;
        }
    } else {
        /* Preserve the original driver's submission order. All workers
         * cooperate on each query before advancing to the next one. */
        schedule->indices[schedule->count++] = drust_query_count - 1;
        schedule->indices[schedule->count++] = drust_query_count - 1;
        for (size_t repeat = 0; repeat < drust_query_repeats; ++repeat) {
            for (size_t query = 0; query + 1 < drust_query_count; ++query) {
                schedule->indices[schedule->count++] = query;
            }
        }
    }
    if (config->passes > SIZE_MAX / schedule->count ||
        config->partition_count > SIZE_MAX / sizeof(group_indices)) {
        printf("Requested query or partition count is too large.\n");
        return -1;
    }
    schedule->query_count = config->passes * schedule->count;
    if (schedule->query_count >
            (SIZE_MAX - sizeof(worker_report)) / sizeof(query_preview) ||
        schedule->query_count > SIZE_MAX / sizeof(query_result)) {
        printf("Requested query count is too large.\n");
        return -1;
    }
    return 0;
}

int main(int argc, char **argv)
{
    benchmark_config config = {.thread_count = default_thread_count,
                               .passes = default_pass_count};
    int argument_status = parse_arguments(argc, argv, &config);
    if (argument_status) {
        print_usage();
        return argument_status < 0;
    }
    query_schedule schedule = {0};
    if (prepare_query_schedule(&config, &schedule)) {
        return 1;
    }

    double application_begin_ms = elapsed_ms(0.0);
    off_t data_begin, file_end;
    if (read_csv_bounds(&config.path, &data_begin, &file_end)) {
        printf("Cannot read a nine-column H2O CSV header and input.\n");
        return 1;
    }
    off_t shard_bytes = (file_end - data_begin) / (off_t)config.thread_count;
    size_t extra_bytes =
        (size_t)((file_end - data_begin) % (off_t)config.thread_count);
    worker_input *first = NULL;
    for (size_t remaining = config.thread_count; remaining; --remaining) {
        worker_input *input = malloc(sizeof(*input));
        if (!input) {
            free_worker_inputs(first);
            printf("Cannot allocate worker arguments.\n");
            return 1;
        }
        size_t worker_id = remaining - 1;
        off_t begin =
            data_begin + (off_t)worker_id * shard_bytes +
            (off_t)(worker_id < extra_bytes ? worker_id : extra_bytes);
        *input = (worker_input){
            .next = first,
            .path = &config.path,
            .queries = query_specs,
            .schedule = schedule.indices,
            .worker_id = worker_id,
            .range = {begin, begin + shard_bytes + (worker_id < extra_bytes)},
            .file_end = file_end,
            .thread_count = config.thread_count,
            .partition_count = config.partition_count,
            .schedule_count = schedule.count,
            .query_count = schedule.query_count};
        first = input;
    }
    if (pthread_barrier_init(&worker_barrier, NULL,
                             (unsigned)config.thread_count)) {
        free_worker_inputs(first);
        printf("Cannot initialize the worker barrier.\n");
        return 1;
    }
    worker_inputs = first;
    /* Entry-block handle array matches the compiler's create/join model.
     * Its capacity is fixed, while the number of workers is a CLI parameter.
     * It is main-private and is never passed to a worker as shared data. */
    pthread_t workers[max_worker_threads];
    void *returned[max_worker_threads];
    size_t started = 0;
    const worker_input *input = first;
    for (; started < config.thread_count; ++started) {
        /* Read this node before handing it to its worker. Main does not
         * touch the node again until every worker has joined. */
        const worker_input *next = input->next;
        if (pthread_create(&workers[started], NULL, worker_entry,
                           (void *)input)) {
            /* A partial cohort cannot use the N-worker barrier. Returning
             * from main ends this run, including workers already waiting.
             * Do not destroy their barrier or free their live arguments. */
            printf("Cannot create all workers.\n");
            return 1;
        }
        input = next;
    }

    int join_failed = 0;
    for (size_t index = 0; index < started; ++index) {
        join_failed |= pthread_join(workers[index], &returned[index]);
    }
    if (join_failed) {
        /* An unjoined thread may still reference its arguments and globals. */
        printf("Cannot join all workers.\n");
        return 1;
    }
    /* This return-pointer array is main-private. No worker receives it, and
     * no returned report is inspected before the complete join loop. Even
     * conservative aliasing of reports does not introduce concurrent writes. */
    worker_report *reports = NULL;
    int error = error_none;
    for (size_t index = 0; index < started; ++index) {
        worker_report *report = returned[index];
        if (report) {
            report->next = reports;
            reports = report;
            if (report->error) {
                error = report->error;
            }
        } else {
            error = error ? error : error_memory;
        }
    }
    pthread_barrier_destroy(&worker_barrier);

    size_t row_count = 0;
    double begin_ms = DBL_MAX, end_ms = 0.0;
    for (const worker_report *report = reports; report; report = report->next) {
        row_count += report->input_rows;
        if (report->query_begin_ms < begin_ms) {
            begin_ms = report->query_begin_ms;
        }
        if (report->query_end_ms > end_ms) {
            end_ms = report->query_end_ms;
        }
    }
    if (!error) {
        printf(
            "engine=polars_c rows=%zu threads=%zu partitions=%zu queries=%zu\n",
            row_count, config.thread_count, config.partition_count,
            schedule.query_count);
        print_query_results(query_specs, schedule.indices, schedule.count,
                            schedule.query_count, reports);
    }
    free_worker_reports(reports);
    free_worker_inputs(first);
    if (error) {
        printf("dataframe failed: error=%d (1=input, 2=allocation, 3=peer).\n",
               error);
        return 1;
    }
    double query_ms = end_ms - begin_ms;
    double application_ms = elapsed_ms(application_begin_ms);
    double processed_rows = (double)row_count * (double)schedule.query_count;
    printf("total_queries=%zu processed_rows=%.0f query_ms=%.6f "
           "query_throughput_rows_per_sec=%.3f application_ms=%.6f "
           "application_throughput_rows_per_sec=%.3f\n",
           schedule.query_count, processed_rows, query_ms,
           query_ms > 0.0 ? processed_rows * 1000.0 / query_ms : 0.0,
           application_ms,
           application_ms > 0.0 ? processed_rows * 1000.0 / application_ms :
                                  0.0);
    return 0;
}
