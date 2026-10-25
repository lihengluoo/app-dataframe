#ifndef dataframe_h
#define dataframe_h

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#define key_columns 6
#define input_columns 9
#define max_aggregates 4
#define max_worker_threads 4096
#define preview_rows 5
#define preview_line_bytes 256
#define csv_buffer_bytes 65536
#define initial_group_capacity 4
#define numeric_thread_min_rows 1000
#define encoded_column_bytes 5
#define encoded_uint32_bytes 4
#define numeric_hash_multiplier UINT64_C(0x55fbfd6bfc5458e9)
#define encoded_hash_basis UINT64_C(0xcbf29ce484222325)
#define encoded_hash_multiplier UINT64_C(0x100000001b3)
#define row_shard_shift 48
#define default_thread_count 8
#define default_pass_count 1
#define drust_query_count 10
#define extended_query_count 13
#define query_spec_count 13
#define drust_query_repeats 3
#define drust_schedule_count (2 + (drust_query_count - 1) * drust_query_repeats)

#define column_id1 0
#define column_id2 1
#define column_id3 2
#define column_id4 3
#define column_id5 4
#define column_id6 5
#define column_v1 6
#define column_v2 7
#define column_v3 8

#define aggregate_sum 0
#define aggregate_min 1
#define aggregate_max 2
#define aggregate_count_nonnull 3
#define aggregate_count_all 4
#define aggregate_mean 5
#define aggregate_type_count 6

#define error_none 0
#define error_input 1
#define error_memory 2
#define error_peer 3

#define suite_drust 0
#define suite_extended 1

typedef size_t column_id;
typedef int aggregate_type;

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
 * publication. No query writes these arrays. Release follows the last query. */
typedef struct {
    size_t rows;
    uint32_t *keys;
    int32_t *integers;
    double *reals;
    uint16_t *valid;
} resident_frame;

/* Composite keys stay in original row order, never hash-routed. The owner
 * publishes bytes and hashes before partition tasks scan all key slices. */
typedef struct {
    unsigned char *bytes;
    uint64_t *hashes;
    int error;
} encoded_keys;

typedef struct {
    uint64_t hash;
    size_t group;
} hash_slot;

/* GroupsIdx.first and GroupsIdx.all[g]. A singleton keeps its row inline,
 * like Polars' unitvec; larger groups own a growable original-row list. */
typedef struct {
    size_t first, count, row_capacity;
    size_t *row_ids;
} group_rows;

typedef struct {
    hash_slot *slots;
    size_t slot_capacity;
    group_rows *groups;
    size_t group_count, group_capacity;
} group_indices;

/* Private directory of immutable original shards; never published to peers.
 * A row ID encodes (original shard, row within shard), not a copied measure.
 * This is equivalent to the contiguous original row index plus shard offsets. */
typedef struct {
    resident_frame *shards;
    encoded_keys *keys;
    size_t shard_count, rows;
    const query_spec *query;
} group_input;

/* Full output chains never escape their worker. Reports contain only copies. */
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

/* Main completes the argument chain before create. Parameters never change.
 * Frames and input errors publish once. Composite keys publish before scans;
 * completed partition groups publish before parallel reduction over groups. */
typedef struct worker_input {
    struct worker_input *next;
    resident_frame frame;
    encoded_keys keys;
    group_indices *partitions;
    size_t group_partition_count;
    int error, query_error;
    const input_path *path;
    const query_spec *queries;
    const size_t *schedule;
    input_range range;
    off_t file_end;
    size_t worker_id, thread_count, partition_count;
    size_t schedule_count, query_count;
} worker_input;

typedef int query_suite;

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

const char column_names[input_columns][4] = {
    "id1", "id2", "id3", "id4", "id5", "id6", "v1", "v2", "v3"};
const char aggregate_names[aggregate_type_count][12] = {
    "sum", "min", "max", "count", "count_all", "mean"};

/* DRust dataframe-ori query operators, with numeric column indices.
 * All workers cooperate on each query. Partition tasks scan original keys
 * and build complete group indices before any aggregation starts.
 * Q11..Q13 exercise additional reductions on the same execution path. */
const query_spec query_specs[query_spec_count] =
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
                     {.type = aggregate_count_nonnull, .column = column_v3},
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
                     {.type = aggregate_count_nonnull, .column = column_v3}}}};

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
size_t original_row_offset(size_t row_id);
const resident_frame *original_shard(const group_input *input, size_t row_id);
number original_measure(const resident_frame *shard, size_t row,
                        column_id column);
uint64_t mix_hash(uint64_t hash);
size_t key_partition(uint64_t hash, size_t partitions);
uint64_t hash_numeric_key(const resident_frame *frame, size_t row,
                          column_id column);
uint64_t hash_key_bytes(const unsigned char *bytes, size_t count);
int encode_group_keys(const resident_frame *frame, const query_spec *query,
                      encoded_keys *keys);
int group_keys_equal(const group_input *input, size_t left_id, size_t right_id);
int resize_group_hash(group_indices *groups, size_t capacity);
int append_group_row(group_rows *group, size_t row_id);
int index_group_row(const group_input *input, group_indices *groups,
                    size_t row_id, uint64_t hash);
void free_group_indices(group_indices *groups);
number aggregate_group(const group_input *input, const group_indices *groups,
                       size_t group, const aggregate_spec *agg, int *valid);
result_frame *append_result(query_result *result, const query_spec *query,
                            size_t groups);
size_t append_field(char *dst, size_t capacity, size_t used,
                    const char *format, ...);
void format_result_row(char *dst, size_t capacity, const result_frame *frame,
                       const query_spec *query, size_t group);
void materialize_keys(const group_input *input, const group_indices *groups,
                      result_frame *result);
int materialize_groups(const group_input *input, const group_indices *groups,
                       query_result *result, query_preview *preview);
int group_partition(const group_input *input, size_t partition,
                    size_t partition_count, group_indices *groups);
int build_partition_groups(worker_input *worker, const group_input *input,
                           size_t partition_count);
int collect_group_slice(const worker_input *worker, group_indices *groups);
void free_partition_groups(worker_input *worker);
void free_query_results(query_result *results, size_t query_count);
int read_input_shards(group_input *input);
int read_encoded_keys(group_input *input);
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
