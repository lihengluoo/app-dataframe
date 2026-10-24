/*
 * Numeric C port of Polars' group-index and aggregation kernels, rs-0.51.0.
 *
 * Upstream, https://github.com/pola-rs/polars/tree/rs-0.51.0:
 *   crates/polars-core/src/frame/group_by/hashing.rs: group_by
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

query_layout prepare_query_layout(const query_spec *query)
{
    query_layout layout = {0};
    for (size_t key = 0; key < query->key_count; ++key) {
        layout.key_set |= (size_t)1 << query->keys[key];
    }
    layout.packet_words = 3 + (query->key_count + 1) / 2;
    return layout;
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

/* The hash-table backend is the C adaptation, not Polars' hashbrown backend.
 * Re-mixing before the table mask avoids reusing partition-selected low bits. */
uint64_t mix_hash(uint64_t hash)
{
    hash ^= hash >> 30;
    hash *= UINT64_C(0xbf58476d1ce4e5b9);
    hash ^= hash >> 27;
    hash *= UINT64_C(0x94d049bb133111eb);
    return hash ^ (hash >> 31);
}

/* All keys use the same tuple hash to select a partition. */
size_t key_partition(uint64_t hash, size_t partitions)
{
    return (size_t)(hash % partitions);
}

uint32_t packet_key(const uint64_t *packet, size_t key)
{
    return (uint32_t)(packet[3 + key / 2] >> (32 * (key % 2)));
}

int group_keys_equal(const query_spec *query, const uint64_t *packet,
                     const group_span *span)
{
    if (packet[2] != span->valid) {
        return 0;
    }
    for (size_t key = 0; key < query->key_count; ++key) {
        if (packet_key(packet, key) != span->keys[key]) {
            return 0;
        }
    }
    return 1;
}

int resize_group_hash(group_indices *groups, size_t capacity)
{
    hash_slot *slots = groups->slots;
    if (capacity > groups->slot_capacity) {
        if (capacity > SIZE_MAX / sizeof(*slots)) {
            return error_memory;
        }
        slots = malloc(capacity * sizeof(*slots));
        if (!slots) {
            return error_memory;
        }
        free(groups->slots);
        groups->slots = slots;
        groups->slot_capacity = capacity;
    }
    for (size_t index = 0; index < capacity; ++index) {
        slots[index].group = SIZE_MAX;
    }
    for (size_t index = 0; index < groups->group_count; ++index) {
        uint64_t hash = groups->spans[index].hash;
        size_t slot = (size_t)mix_hash(hash) & (capacity - 1);
        while (slots[slot].group != SIZE_MAX) {
            slot = (slot + 1) & (capacity - 1);
        }
        slots[slot] = (hash_slot){.hash = hash, .group = index};
    }
    return error_none;
}

/* Reuse the last capacity for these grouping columns, not the contents.
 * A repeated high-cardinality query avoids the entire doubling sequence;
 * a low-cardinality query still clears only its own small active table. */
int prepare_group_indices(const group_input *input, group_indices *groups)
{
    groups->group_count = 0;
    groups->active_slots = groups->hash_sizes[input->layout->key_set];
    if (groups->active_slots == 0) {
        groups->active_slots = 64;
    }
    if (input->rows == 0) {
        return error_none;
    }
    if (input->rows > groups->row_capacity) {
        if (input->rows > SIZE_MAX / sizeof(group_member)) {
            return error_memory;
        }
        group_member *rows = malloc(input->rows * sizeof(*rows));
        if (!rows) {
            return error_memory;
        }
        free(groups->members);
        groups->members = rows;
        groups->row_capacity = input->rows;
    }
    return resize_group_hash(groups, groups->active_slots);
}

/* Polars hashing.rs::group_by: a new key starts a row-ID list; an existing
 * key appends to its list. Read incoming keys directly while B1/B2 protects
 * the packet; only a new group's representative key is copied privately.
 * No packet pointer is retained. Collisions compare the complete key. */
int index_group_row(const group_input *input, group_indices *groups, size_t row,
                    const uint64_t *packet)
{
    uint64_t hash = packet[0];
    size_t slot = (size_t)mix_hash(hash) & (groups->active_slots - 1);
    while (groups->slots[slot].group != SIZE_MAX) {
        size_t group = groups->slots[slot].group;
        if (groups->slots[slot].hash == hash &&
            group_keys_equal(input->query, packet, &groups->spans[group])) {
            break;
        }
        slot = (slot + 1) & (groups->active_slots - 1);
    }
    if (groups->slots[slot].group == SIZE_MAX) {
        if (groups->group_count + 1 >
            groups->active_slots - groups->active_slots / 4) {
            if (groups->active_slots > SIZE_MAX / 2 ||
                resize_group_hash(groups, groups->active_slots * 2)) {
                return error_memory;
            }
            groups->active_slots *= 2;
            slot = (size_t)mix_hash(hash) & (groups->active_slots - 1);
            while (groups->slots[slot].group != SIZE_MAX) {
                slot = (slot + 1) & (groups->active_slots - 1);
            }
        }
        if (groups->group_count == groups->group_capacity) {
            size_t capacity =
                groups->group_capacity ? groups->group_capacity * 2 : 64;
            if (capacity < groups->group_capacity ||
                capacity > SIZE_MAX / sizeof(group_span)) {
                return error_memory;
            }
            group_span *spans = malloc(capacity * sizeof(*spans));
            if (!spans) {
                return error_memory;
            }
            for (size_t index = 0; index < groups->group_count; ++index) {
                spans[index] = groups->spans[index];
            }
            free(groups->spans);
            groups->spans = spans;
            groups->group_capacity = capacity;
        }
        size_t group = groups->group_count++;
        groups->spans[group] = (group_span){
            .first = packet[1], .hash = hash, .valid = (uint16_t)packet[2]};
        groups->slots[slot] = (hash_slot){.hash = hash, .group = group};
        for (size_t key = 0; key < input->query->key_count; ++key) {
            groups->spans[group].keys[key] = packet_key(packet, key);
        }
    }
    size_t group = groups->slots[slot].group;
    groups->members[row] = (group_member){.group = group, .row_id = packet[1]};
    ++groups->spans[group].count;
    return error_none;
}

/* Packed equivalent of GroupsIdx: first is the representative row, and
 * row_ids[begin .. begin+count) holds every original member, including all
 * singleton groups. No query reuses another query's group contents. */
int pack_group_indices(const group_input *input, group_indices *groups)
{
    if (input->rows > groups->id_capacity) {
        size_t *ids = malloc(input->rows * sizeof(*ids));
        if (!ids) {
            return error_memory;
        }
        free(groups->row_ids);
        groups->row_ids = ids;
        groups->id_capacity = input->rows;
    }
    size_t begin = 0;
    for (size_t group = 0; group < groups->group_count; ++group) {
        group_span *span = &groups->spans[group];
        span->begin = begin;
        begin += span->count;
        span->count = 0;
    }
    for (size_t row = 0; row < input->rows; ++row) {
        group_span *span = &groups->spans[groups->members[row].group];
        groups->row_ids[span->begin + span->count++] =
            groups->members[row].row_id;
    }
    return error_none;
}

void free_group_indices(group_indices *groups)
{
    free(groups->slots);
    free(groups->spans);
    free(groups->members);
    free(groups->row_ids);
}

/* Each aggregate makes its own complete traversal of original row IDs.
 * Like DRust, SUM/MIN produce Float64 even for an Int32 input column.
 * Original cells have fixed types; no temporary byte vector is allocated. */
number aggregate_group(const group_input *input, const group_indices *groups,
                       size_t group, const aggregate_spec *agg, int *valid)
{
    const group_span *span = &groups->spans[group];
    size_t count = 0;
    double accumulated = 0.0;
    for (size_t offset = 0; offset < span->count; ++offset) {
        size_t row_id = groups->row_ids[span->begin + offset];
        const resident_frame *shard = original_shard(input, row_id);
        size_t row = original_row_offset(row_id);
        if (agg->type == aggregate_count_all) {
            ++count;
            continue;
        }
        if (!(shard->valid[row] & (1u << agg->column))) {
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
             agg->type == aggregate_count || agg->type == aggregate_count_all;
    if (agg->type == aggregate_count || agg->type == aggregate_count_all) {
        return (number){.bits = count};
    }
    if (agg->type == aggregate_mean && count) {
        accumulated /= (double)count;
    }
    return (number){.real = accumulated};
}

/* Only the producer calls this appender and writes the returned packet.
 * One flat buffer per partition doubles on demand; its capacity survives
 * across queries while the row count resets after the read-done barrier. */
uint64_t *append_exchange(exchange_partition *partition, size_t stride)
{
    if (partition->rows == SIZE_MAX / stride) {
        return NULL;
    }
    size_t needed = (partition->rows + 1) * stride;
    if (needed > partition->word_capacity) {
        size_t capacity = partition->word_capacity * 2;
        if (capacity < needed) {
            capacity = needed;
        }
        if (capacity > SIZE_MAX / sizeof(uint64_t)) {
            return NULL;
        }
        uint64_t *words = malloc(capacity * sizeof(*words));
        if (!words) {
            return NULL;
        }
        if (partition->rows) {
            memcpy(words, partition->words, partition->rows * stride *
                          sizeof(*words));
        }
        free(partition->words);
        partition->words = words;
        partition->word_capacity = capacity;
    }
    uint64_t *packet = partition->words + partition->rows * stride;
    ++partition->rows;
    return packet;
}

/* Read the resident shard once for this query. Each raw row goes to exactly
 * one hash partition; only selected keys and the original row ID travel. */
int produce_partitions(const resident_frame *frame, const query_spec *query,
                       const query_layout *layout, worker_mailbox *mailbox,
                       size_t partitions, size_t worker_id)
{
    for (size_t row = 0; row < frame->rows; ++row) {
        uint64_t keys[key_columns], mask = 0;
        uint16_t valid = frame->valid[row];
        for (size_t key = 0; key < query->key_count; ++key) {
            size_t column = query->keys[key];
            unsigned present = (valid >> column) & 1;
            mask |= (uint64_t)present << key;
            keys[key] = present ? frame->keys[column * frame->rows + row] : 0;
        }
        uint64_t hash = mix_hash(mask + UINT64_C(0x9e3779b97f4a7c15));
        for (size_t key = 0; key < query->key_count; ++key) {
            hash = mix_hash(hash ^ mix_hash(keys[key] + key +
                                            UINT64_C(0x9e3779b97f4a7c15)));
        }
        size_t partition = key_partition(hash, partitions);
        uint64_t *packet = append_exchange(&mailbox->partitions[partition],
                                           layout->packet_words);
        if (!packet) {
            return error_memory;
        }
        packet[0] = hash;
        packet[1] = (worker_id << row_shard_shift) | row;
        packet[2] = mask;
        for (size_t key = 0; key < query->key_count; key += 2) {
            packet[3 + key / 2] = keys[key];
            if (key + 1 < query->key_count) {
                packet[3 + key / 2] |= keys[key + 1] << 32;
            }
        }
    }
    return error_none;
}

/* Full columnar output, allocated exactly once per partition/query. Numeric
 * columns are doubles (except explicit counts); keys retain UInt32. Results
 * stay alive through the last query, just like the Rust result DataFrames. */
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
static size_t append_field(char *dst, size_t capacity, size_t used,
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
        if (agg->type == aggregate_count || agg->type == aggregate_count_all) {
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
            size_t row_id = groups->spans[group].first;
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

/* Build the complete key -> ORIGINAL row IDs map without reading measures.
 * Cached tuple keys are for hash collision comparisons only. Output keys
 * are taken again from the original key columns in materialize_keys. */
int gather_partition(size_t partition, group_input *input,
                     group_indices *groups)
{
    size_t stride = input->layout->packet_words;
    input->rows = 0;
    for (const worker_input *worker = worker_inputs; worker;
         worker = worker->next) {
        size_t count = worker->mailbox->partitions[partition].rows;
        if (count > SIZE_MAX - input->rows) {
            return error_memory;
        }
        input->rows += count;
    }
    int error = prepare_group_indices(input, groups);
    if (error) {
        return error;
    }
    size_t row = 0;
    for (const worker_input *worker = worker_inputs; worker;
         worker = worker->next) {
        const exchange_partition *part = &worker->mailbox->partitions[partition];
        for (size_t index = 0; index < part->rows; ++index) {
            error = index_group_row(input, groups, row++,
                                    part->words + index * stride);
            if (error) {
                return error;
            }
        }
    }
    groups->hash_sizes[input->layout->key_set] = groups->active_slots;
    return pack_group_indices(input, groups);
}

int consume_partitions(const worker_input *worker, group_input *input,
                       group_indices *groups, query_result *result,
                       query_preview *preview)
{
    for (const worker_input *peer_input = worker_inputs; peer_input;
         peer_input = peer_input->next) {
        const worker_mailbox *peer = peer_input->mailbox;
        if (!peer || peer->error) {
            return error_peer;
        }
    }
    for (size_t partition = worker->worker_id;
         partition < worker->partition_count;
         partition += worker->thread_count) {
        int error = gather_partition(partition, input, groups);
        if (!error) {
            error =
                materialize_groups(input, groups, result, preview);
        }
        if (error) {
            return error;
        }
    }
    return error_none;
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

void free_exchange(worker_mailbox *mailbox, size_t partitions)
{
    if (!mailbox) {
        return;
    }
    for (size_t partition = 0; partition < partitions; ++partition) {
        free(mailbox->partitions[partition].words);
    }
    free(mailbox);
}

/* All barrier waits live here. No computational helper waits, creates a
 * thread, or reads a peer mailbox during the producer phase. Main initializes
 * the barrier and all parameters before create; every worker has the same
 * query_count and executes the same unconditional waits.
 *
 * Owner publication  -> B0: original columns and mailbox pointers are readable.
 * Producer writes     -> B1: consumers may read exchange blocks.
 * Consumer reads      -> B2: owners may reuse those blocks in the next query.
 * The final B2 also precedes all original-column/mailbox/payload destruction.
 * I/O/OOM errors suppress computation but never skip a loop barrier.
 */
void *worker_entry(void *argument)
{
    worker_input *input = argument;

    /* Keep owner pointers directly rooted at this worker's malloc calls.
     * Publication does not change ownership; the report escapes only at join. */
    worker_mailbox *mailbox = malloc(
        sizeof(*mailbox) + input->partition_count * sizeof(exchange_partition));
    worker_report *report =
        malloc(sizeof(*report) + input->query_count * sizeof(query_preview));
    query_result *results = malloc(input->query_count * sizeof(*results));
    int error = error_none;
    if (!mailbox || !report || !results) {
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
    if (mailbox) {
        *mailbox = (worker_mailbox){0};
        for (size_t partition = 0; partition < input->partition_count;
             ++partition) {
            mailbox->partitions[partition] = (exchange_partition){0};
        }
    }
    input->mailbox = mailbox;
    group_indices groups = {0};
    group_input received = {
        .shards = malloc(input->thread_count * sizeof(resident_frame))};
    if (!received.shards) {
        error = error_memory;
    }
    if (!error) {
        error = load_input_shard(input, &input->frame);
    }

    pthread_barrier_wait(&worker_barrier); /* B0: publication and input done. */
    double query_begin_ms = elapsed_ms(0.0);
    if (received.shards) {
        for (const worker_input *peer = worker_inputs; peer;
             peer = peer->next) {
            received.shards[peer->worker_id] = peer->frame;
        }
    }

    for (size_t query_index = 0; query_index < input->query_count;
         ++query_index) {
        size_t op = input->schedule[query_index % input->schedule_count];
        const query_spec *query = &input->queries[op];
        query_layout layout = prepare_query_layout(query);
        received.query = query;
        received.layout = &layout;
        if (mailbox) {
            for (size_t partition = 0; partition < input->partition_count;
                 ++partition) {
                mailbox->partitions[partition].rows = 0;
            }
            if (!error) {
                error = produce_partitions(&input->frame, query, &layout,
                                           mailbox, input->partition_count,
                                           input->worker_id);
            }
            mailbox->error = error;
        }

        pthread_barrier_wait(&worker_barrier); /* B1: exchange is read-only. */

        if (!error) {
            error = consume_partitions(input, &received, &groups,
                                       &results[query_index],
                                       &report->queries[query_index]);
        }

        pthread_barrier_wait(
            &worker_barrier); /* B2: all packet readers done. */
    }
    /* Keep final workspace and exchange cleanup inside the same continuous
     * query interval. The final B2 has ended every shared-input/packet read. */
    free_group_indices(&groups);
    free(received.shards);
    free_exchange(mailbox, input->partition_count);
    double query_end_ms = elapsed_ms(0.0);

    /* Inputs and complete outputs live through query timing, as in the Rust
     * comparison driver. Their final release is included in application_ms. */
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
        config->partition_count >
            (SIZE_MAX - sizeof(worker_mailbox)) / sizeof(exchange_partition)) {
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
