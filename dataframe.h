#ifndef dataframe_h
#define dataframe_h

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

enum {
    key_columns = 6,
    input_columns = 9,
    max_aggregates = 4,
    max_worker_threads = 4096,
    preview_rows = 5,
    preview_line_bytes = 256,
    csv_buffer_bytes = 65536,
    initial_partition_words = 256,
    row_shard_shift = 48,
    default_thread_count = 8,
    default_pass_count = 1,
    drust_query_count = 10,
    extended_query_count = 13,
    query_spec_count = 13,
    drust_query_repeats = 3,
    drust_schedule_count = 2 + (drust_query_count - 1) * drust_query_repeats
};

typedef enum {
    column_id1,
    column_id2,
    column_id3,
    column_id4,
    column_id5,
    column_id6,
    column_v1,
    column_v2,
    column_v3
} column_id;

typedef enum {
    aggregate_sum,
    aggregate_min,
    aggregate_max,
    aggregate_count,
    aggregate_count_all,
    aggregate_mean,
    aggregate_type_count
} aggregate_type;

typedef enum { error_none, error_input, error_memory, error_peer } error_code;

typedef union {
    uint64_t bits;
    int64_t integer;
    double real;
} number;

typedef struct {
    aggregate_type type;
    column_id column;
} aggregate_spec;

/* Query descriptions are immutable and contain no pointers. */
typedef struct {
    size_t id;
    size_t key_count;
    column_id keys[key_columns];
    size_t aggregate_count;
    aggregate_spec aggregates[max_aggregates];
} query_spec;

typedef struct {
    char bytes[4096];
} input_path;

typedef struct {
    int fd;
    off_t position, end;
    size_t cursor, count;
    unsigned char bytes[csv_buffer_bytes];
} csv_reader;

typedef struct {
    off_t begin, end;
} input_range;

/* Original typed columns: owner writes during loading; all workers read after
 * B0. No query writes these arrays. The owner frees them after the final B2. */
typedef struct {
    size_t rows;
    uint32_t *keys;
    int32_t *integers;
    double *reals;
    uint16_t *valid;
} resident_frame;

/* Packet: hash, original row ID, key validity, pairs of UInt32 keys.
 * Measures are accessed only through the original columns during aggregation. */
typedef struct {
    size_t packet_words;
    size_t key_set;
} query_layout;

typedef struct {
    uint64_t *words;
    size_t rows, word_capacity;
} exchange_partition;

/* One direct worker malloc owns this directory and its partition entries.
 * Each partition's payload buffer is a separate malloc; no large shared
 * parent payload. All mutable partition fields are written in phase 1,
 * read in phase 2. */
typedef struct worker_mailbox {
    int error;
    exchange_partition partitions[];
} worker_mailbox;

typedef struct {
    uint64_t hash;
    size_t group;
} hash_slot;

/* Packed equivalent of GroupsIdx.first and GroupsIdx.all[g]. */
typedef struct {
    size_t first, begin, count;
    uint64_t hash;
    uint32_t keys[key_columns];
    uint16_t valid;
} group_span;

typedef struct {
    size_t group, row_id;
} group_member;

typedef struct {
    hash_slot *slots;
    size_t slot_capacity, active_slots;
    group_span *spans;
    size_t group_count, group_capacity;
    group_member *members;
    size_t *row_ids;
    size_t row_capacity, id_capacity;
    /* Private capacity hints, indexed by the set of grouping columns.
     * Only sizes survive; every query rebuilds its groups and row indices. */
    size_t hash_sizes[1 << key_columns];
} group_indices;

/* Private directory of immutable original shards; never published to peers.
 * A row ID encodes (original shard, row within shard), not a copied measure.
 * This is equivalent to the contiguous original row index plus shard offsets. */
typedef struct {
    resident_frame *shards;
    size_t rows;
    const query_spec *query;
    const query_layout *layout;
} group_input;

/* Full output chains never escape their worker. Their allocation site is
 * separate from the shared exchange and preview report allocation sites. */
typedef struct result_frame {
    struct result_frame *next;
    size_t groups;
    number *aggregates;
    uint32_t *keys;
    uint16_t *valid;
} result_frame;

typedef struct {
    result_frame *first, *last;
    size_t groups;
} query_result;

/* Only scalar copies cross the join boundary, never full-result pointers.
 * Each preview row is a finished text line; main prints it unchanged. */
typedef struct {
    size_t groups, rows;
    char lines[preview_rows][preview_line_bytes];
} query_preview;

typedef struct worker_report {
    struct worker_report *next;
    int error;
    size_t input_rows;
    double query_begin_ms, query_end_ms;
    query_preview queries[];
} worker_report;

/* Main allocates each argument independently and completes the chain before
 * create. Parameters and next never change afterward. Only the corresponding
 * worker initializes frame and mailbox before B0; peers read only after B0.
 * This reuses an existing object instead of adding publication slots. */
typedef struct worker_input {
    struct worker_input *next;
    worker_mailbox *mailbox;
    resident_frame frame;
    const input_path *path;
    const query_spec *queries;
    const size_t *schedule;
    input_range range;
    off_t file_end;
    size_t worker_id, thread_count, partition_count;
    size_t schedule_count, query_count;
} worker_input;

typedef enum { suite_drust, suite_extended } query_suite;

typedef struct {
    input_path path;
    size_t thread_count, partition_count;
    size_t passes, selected_query;
    query_suite suite;
} benchmark_config;

/* Indices select query_specs; count is per pass, query_count includes passes. */
typedef struct {
    size_t indices[drust_schedule_count];
    size_t count, query_count;
} query_schedule;

static const char column_names[input_columns][4] = {
    "id1", "id2", "id3", "id4", "id5", "id6", "v1", "v2", "v3"};
static const char aggregate_names[aggregate_type_count][12] = {
    "sum", "min", "max", "count", "count_all", "mean"};

/* DRust dataframe-ori query operators, with numeric enums replacing
 * column-name strings. Query concurrency is deliberately a separate
 * choice: all persistent workers cooperate on each occurrence here.
 * Every query exchanges keys/row IDs and builds complete group indices.
 * Q11..Q13 exercise additional reductions on the same execution path.
 */
static const query_spec query_specs[query_spec_count] =
    {{.id = 1,
      .key_count = 1,
      .keys = {column_id1},
      .aggregate_count = 1,
      .aggregates = {{.type = aggregate_sum, .column = column_v1}}},
     {.id = 2,
      .key_count = 2,
      .keys = {column_id1, column_id2},
      .aggregate_count = 1,
      .aggregates = {{.type = aggregate_sum, .column = column_v1}}},
     {.id = 3,
      .key_count = 1,
      .keys = {column_id3},
      .aggregate_count = 2,
      .aggregates = {{.type = aggregate_sum, .column = column_v1},
                     {.type = aggregate_sum, .column = column_v3}}},
     {.id = 4,
      .key_count = 1,
      .keys = {column_id4},
      .aggregate_count = 2,
      .aggregates = {{.type = aggregate_sum, .column = column_v1},
                     {.type = aggregate_sum, .column = column_v3}}},
     {.id = 5,
      .key_count = 1,
      .keys = {column_id6},
      .aggregate_count = 2,
      .aggregates = {{.type = aggregate_sum, .column = column_v1},
                     {.type = aggregate_sum, .column = column_v3}}},
     {.id = 6,
      .key_count = 2,
      .keys = {column_id4, column_id5},
      .aggregate_count = 2,
      .aggregates = {{.type = aggregate_sum, .column = column_v3},
                     {.type = aggregate_min, .column = column_v3}}},
     {.id = 7,
      .key_count = 1,
      .keys = {column_id3},
      .aggregate_count = 2,
      .aggregates = {{.type = aggregate_min, .column = column_v1},
                     {.type = aggregate_min, .column = column_v2}}},
     {.id = 8,
      .key_count = 1,
      .keys = {column_id6},
      .aggregate_count = 1,
      .aggregates = {{.type = aggregate_min, .column = column_v3}}},
     {.id = 9,
      .key_count = 2,
      .keys = {column_id2, column_id4},
      .aggregate_count = 2,
      .aggregates = {{.type = aggregate_sum, .column = column_v1},
                     {.type = aggregate_sum, .column = column_v2}}},
     {.id = 10,
      .key_count = 6,
      .keys = {column_id1, column_id2, column_id3, column_id4, column_id5,
               column_id6},
      .aggregate_count = 2,
      .aggregates = {{.type = aggregate_sum, .column = column_v1},
                     {.type = aggregate_sum, .column = column_v3}}},
     {.id = 11,
      .key_count = 1,
      .keys = {column_id1},
      .aggregate_count = 4,
      .aggregates = {{.type = aggregate_mean, .column = column_v1},
                     {.type = aggregate_mean, .column = column_v3},
                     {.type = aggregate_count, .column = column_v3},
                     {.type = aggregate_max, .column = column_v2}}},
     {.id = 12,
      .key_count = 2,
      .keys = {column_id4, column_id5},
      .aggregate_count = 4,
      .aggregates = {{.type = aggregate_sum, .column = column_v1},
                     {.type = aggregate_min, .column = column_v2},
                     {.type = aggregate_max, .column = column_v3},
                     {.type = aggregate_count_all, .column = column_v1}}},
     {.id = 13,
      .key_count = 1,
      .keys = {column_id3},
      .aggregate_count = 2,
      .aggregates = {{.type = aggregate_mean, .column = column_v3},
                     {.type = aggregate_count, .column = column_v3}}}};

/* Main initializes these before creating workers and destroys the barrier
 * only after every worker has joined. See worker_entry for phase ordering. */
extern pthread_barrier_t worker_barrier;
extern const worker_input *worker_inputs;

double elapsed_ms(double begin_ms);
int text_equal(const char *left, const char *right);
int copy_argument(input_path *dst, const char *src);
int parse_count(const char *digits, size_t *count);
int read_csv_byte(csv_reader *reader);
int skip_csv_row(csv_reader *reader);
int read_csv_bounds(const input_path *path, off_t *data_begin, off_t *file_end);
int align_csv_boundary(csv_reader *reader, off_t offset, off_t *aligned);
int parse_csv_number(const char *text, size_t column, number *parsed,
                     int *valid);
int read_csv_row(csv_reader *reader, number *cells, uint16_t *valid);
int load_input_shard(const worker_input *input, resident_frame *frame);
query_layout prepare_query_layout(const query_spec *query);
size_t original_row_offset(size_t row_id);
const resident_frame *original_shard(const group_input *input, size_t row_id);
number original_measure(const resident_frame *shard, size_t row,
                        column_id column);
uint64_t mix_hash(uint64_t hash);
size_t key_partition(uint64_t hash, size_t partitions);
uint32_t packet_key(const uint64_t *packet, size_t key);
int group_keys_equal(const query_spec *query, const uint64_t *packet,
                     const group_span *span);
int resize_group_hash(group_indices *groups, size_t capacity);
int prepare_group_indices(const group_input *input, group_indices *groups);
int index_group_row(const group_input *input, group_indices *groups, size_t row,
                    const uint64_t *packet);
int pack_group_indices(const group_input *input, group_indices *groups);
void free_group_indices(group_indices *groups);
number aggregate_group(const group_input *input, const group_indices *groups,
                       size_t group, const aggregate_spec *agg, int *valid);
uint64_t *append_exchange(exchange_partition *partition, size_t stride);
int produce_partitions(const resident_frame *frame, const query_spec *query,
                       const query_layout *layout, worker_mailbox *mailbox,
                       size_t partitions, size_t worker_id);
result_frame *append_result(query_result *result, const query_spec *query,
                            size_t groups);
void format_result_row(char *dst, size_t capacity, const result_frame *frame,
                       const query_spec *query, size_t group);
void materialize_keys(const group_input *input, const group_indices *groups,
                      result_frame *result);
int materialize_groups(const group_input *input, const group_indices *groups,
                       query_result *result, query_preview *preview);
int gather_partition(size_t partition, group_input *input,
                     group_indices *groups);
int consume_partitions(const worker_input *worker, group_input *input,
                       group_indices *groups, query_result *result,
                       query_preview *preview);
void free_query_results(query_result *results, size_t query_count);
void free_exchange(worker_mailbox *mailbox, size_t partitions);
void *worker_entry(void *argument);
void print_query_results(const query_spec *queries, const size_t *schedule,
                         size_t schedule_count, size_t query_count,
                         const worker_report *reports);
void free_worker_reports(worker_report *report);
void free_worker_inputs(worker_input *input);
void print_usage(void);
int parse_arguments(int argc, char **argv, benchmark_config *config);
int prepare_query_schedule(const benchmark_config *config,
                           query_schedule *schedule);

#endif
