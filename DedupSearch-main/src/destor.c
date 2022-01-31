/*
 * server.c
 *
 *  Created on: May 24, 2012
 *      Author: fumin
 */

#include "destor.h"
#include "jcr.h"
#include "index/index.h"
#include "storage/containerstore.h"

extern void do_backup(char *path);
//extern void do_delete(int revision);
extern void do_restore(int revision, char *path);
void do_delete(int jobid);
extern void make_trace(char *raw_files);
extern void do_naive_search(int revision, unsigned char **keywords_p, size_t keywords_num_p);
extern void do_quick_search(int revision, unsigned char **keywords_p, size_t keywords_num_p, sds tiny_path);

extern int load_config();
extern void load_config_from_string(sds config);

struct destor destor; // DedupSearch

/* : means argument is required.
 * :: means argument is required and no space.
 */
const char * const short_options = "sr::t::p::hd::q::n::f::l::"; // USE n OR q AT THE END OF COMMAND

struct option long_options[] = {
		{ "state", 0, NULL, 's' },
		{ "help", 0, NULL, 'h' },
		{ NULL, 0, NULL, 0 }
};

void usage() {
	puts("GENERAL USAGE");
	puts("\tstart a backup job");
	puts("\t\tdestor /path/to/data -p\"a line in config file\"");

	puts("\tstart a restore job");
	puts("\t\tdestor -r<JOB_ID> /path/to/restore -p\"a line in config file\"");

	puts("\tprint state of destor");
	puts("\t\tdestor -s");

	puts("\tprint this");
	puts("\t\tdestor -h");

	puts("\tMake a trance");
	puts("\t\tdestor -t /path/to/data");

	puts("\tParameter");
	puts("\t\t-p\"a line in config file\"");

	puts("\tNaive search");
	puts("\t\tdestor -n<JOB_ID> keyword1 keyword2 ...");
	puts("\t\tUse at end of command only!");

	puts("\tQuick search");
	puts("\t\tdestor -q<JOB_ID> keyword1 keyword2 ...");
	puts("\t\tUse at end of command only!");

	puts("\tTiny directory for Quick search");
	puts("\t\t-d\"path to tiny's directory\"");

	puts("\tKeywords input file for Quick search");
	puts("\t\t-f\"path to keywords file\"");
	exit(0);
}

void destor_log(int level, const char *fmt, ...) {
	va_list ap;
	char msg[DESTOR_MAX_LOGMSG_LEN];

	if ((level & 0xff) < destor.verbosity)
		return;

	va_start(ap, fmt);
	vsnprintf(msg, sizeof(msg), fmt, ap);
	va_end(ap);

	fprintf(stdout, "%s\n", msg);
}

void check_simulation_level(int last_level, int current_level) {
	if ((last_level <= SIMULATION_RESTORE && current_level >= SIMULATION_APPEND)
			|| (last_level >= SIMULATION_APPEND
					&& current_level <= SIMULATION_RESTORE)) {
		fprintf(stderr, "FATAL ERROR: Conflicting simualtion level!\n");
		exit(1);
	}
}

char** get_keywords(sds keywords_file, size_t* keywords_num, size_t keyword_length, int argc, char **argv){
	sds* keywords;
	if (keywords_file != NULL){
		FILE * f = fopen(keywords_file, "rb");
		if (f == NULL){
			fprintf(stderr, "Error with keywords file!\n");
			usage();
		}
		char* keyword_buff = malloc(keyword_length);
		fseek(f, 0, SEEK_END); // seek to end of file
		int size = ftell(f); // get current file pointer
		fseek(f, 0, SEEK_SET); // seek back to beginning of file
		(*keywords_num) = size / keyword_length;
		keywords = malloc((*keywords_num) * sizeof(sds));
		for (int i = 0; i < (*keywords_num); i++){
			fread (keyword_buff, keyword_length, 1, f);
			keywords[i] = sdsnewlen(keyword_buff, keyword_length);
		}
		free(keyword_buff);
		fclose(f);
		sdsfree(keywords_file);
	} else if (argc > optind) {
		(*keywords_num) = argc - optind;
		keywords = malloc((*keywords_num) * sizeof(sds));
		for (int i = 0; i < (*keywords_num); i++, optind++){
			keywords[i] = sdsnew(argv[optind]);
		}
	} else {
		fprintf(stderr, "Keywords are required!\n");
		usage();
	}
	return keywords;
}

void destor_start() {

	/* Init */
	destor.working_directory = sdsnew("/home/data/working/");
	destor.simulation_level = SIMULATION_NO;
    destor.trace_format = TRACE_DESTOR;
	destor.verbosity = DESTOR_WARNING;

	destor.chunk_algorithm = CHUNK_RABIN;
	destor.chunk_max_size = 65536;
	destor.chunk_min_size = 1024;
	destor.chunk_avg_size = 8192;

	destor.restore_cache[0] = RESTORE_CACHE_LRU;
	destor.restore_cache[1] = 1024;
	destor.restore_opt_window_size = 1000000;

	destor.index_category[0] = INDEX_CATEGORY_NEAR_EXACT;
	destor.index_category[1] = INDEX_CATEGORY_PHYSICAL_LOCALITY;
	destor.index_specific = INDEX_SPECIFIC_NO;
	destor.index_key_value_store = INDEX_KEY_VALUE_HTABLE;
	destor.index_key_size = 20;
    destor.index_value_length = 1;
    
	destor.index_cache_size = 4096;

	destor.index_segment_algorithm[0] = INDEX_SEGMENT_FIXED;
	destor.index_segment_algorithm[1] = 1024;
	destor.index_segment_min = 128;
	destor.index_segment_max = 10240;
	destor.index_sampling_method[0] = INDEX_SAMPLING_UNIFORM;
	destor.index_sampling_method[1] = 1;
	destor.index_value_length = 1;
	destor.index_segment_selection_method[0] = INDEX_SEGMENT_SELECT_TOP;
	destor.index_segment_selection_method[1] = 1;
	destor.index_segment_prefech = 0;

	destor.rewrite_algorithm[0] = REWRITE_NO;
	destor.rewrite_algorithm[1] = 1024;

	/* for History-Aware Rewriting (HAR) */
	destor.rewrite_enable_har = 0;
	destor.rewrite_har_utilization_threshold = 0.5;
	destor.rewrite_har_rewrite_limit = 0.05;

	/* for Cache-Aware Filter */
	destor.rewrite_enable_cache_aware = 0;

	/*
	 * Specify how many backups are retained.
	 * A negative value indicates all backups are retained.
	 */
	destor.backup_retention_time = -1;

	load_config();

	sds stat_file = sdsdup(destor.working_directory);
	stat_file = sdscat(stat_file, "/destor.stat");

	FILE *fp;
	if ((fp = fopen(stat_file, "r"))) {

		fread(&destor.chunk_num, 8, 1, fp);
		fread(&destor.stored_chunk_num, 8, 1, fp);

		fread(&destor.data_size, 8, 1, fp);
		fread(&destor.stored_data_size, 8, 1, fp);

		fread(&destor.zero_chunk_num, 8, 1, fp);
		fread(&destor.zero_chunk_size, 8, 1, fp);

		fread(&destor.rewritten_chunk_num, 8, 1, fp);
		fread(&destor.rewritten_chunk_size, 8, 1, fp);

		fread(&destor.index_memory_footprint, 4, 1, fp);

		fread(&destor.live_container_num, 4, 1, fp);

		int last_retention_time;
		fread(&last_retention_time, 4, 1, fp);
		assert(last_retention_time == destor.backup_retention_time);

		int last_level;
		fread(&last_level, 4, 1, fp);
		check_simulation_level(last_level, destor.simulation_level);

		fclose(fp);
	} else {
		destor.chunk_num = 0;
		destor.stored_chunk_num = 0;
		destor.data_size = 0;
		destor.stored_data_size = 0;
		destor.zero_chunk_num = 0;
		destor.zero_chunk_size = 0;
		destor.rewritten_chunk_num = 0;
		destor.rewritten_chunk_size = 0;
		destor.index_memory_footprint = 0;
		destor.live_container_num = 0;
	}

	sdsfree(stat_file);
}

void destor_shutdown() {
	sds stat_file = sdsdup(destor.working_directory);
	stat_file = sdscat(stat_file, "/destor.stat");

	FILE *fp;
	if ((fp = fopen(stat_file, "w")) == 0) {
		destor_log(DESTOR_WARNING, "Fatal error, can not open destor.stat!");
		exit(1);
	}

	fwrite(&destor.chunk_num, 8, 1, fp);
	fwrite(&destor.stored_chunk_num, 8, 1, fp);

	fwrite(&destor.data_size, 8, 1, fp);
	fwrite(&destor.stored_data_size, 8, 1, fp);

	fwrite(&destor.zero_chunk_num, 8, 1, fp);
	fwrite(&destor.zero_chunk_size, 8, 1, fp);

	fwrite(&destor.rewritten_chunk_num, 8, 1, fp);
	fwrite(&destor.rewritten_chunk_size, 8, 1, fp);

	fwrite(&destor.index_memory_footprint, 4, 1, fp);

	fwrite(&destor.live_container_num, 4, 1, fp);

	fwrite(&destor.backup_retention_time, 4, 1, fp);

	fwrite(&destor.simulation_level, 4, 1, fp);

	fclose(fp);
	sdsfree(stat_file);
}

void destor_stat() {
	printf("=== destor stat ===\n");

	printf("the index memory footprint (B): %" PRId32 "\n",
			destor.index_memory_footprint);

	printf("the number of live containers: %" PRId32 "\n",
			destor.live_container_num);

	printf("the number of chunks: %" PRId64 "\n", destor.chunk_num);
	printf("the number of stored chunks: %" PRId64 "\n", destor.stored_chunk_num);

	printf("the size of data (B): %" PRId64 "\n", destor.data_size);
	printf("the size of stored data (B): %" PRId64 "\n", destor.stored_data_size);

	printf("the size of saved data (B): %" PRId64 "\n",
			destor.data_size - destor.stored_data_size);

	printf("deduplication ratio: %.4f, %.4f\n",
			(destor.data_size - destor.stored_data_size)
					/ (double) destor.data_size,
			((double) destor.data_size) / (destor.stored_data_size));

	printf("the number of zero chunks: %" PRId64 "\n", destor.zero_chunk_num);
	printf("the size of zero chunks (B): %" PRId64 "\n", destor.zero_chunk_size);

	printf("the number of rewritten chunks: %" PRId64 "\n", destor.rewritten_chunk_num);
	printf("the size of rewritten chunks (B): %" PRId64 "\n",
			destor.rewritten_chunk_size);
	printf("rewrite ratio: %.4f\n",
			destor.rewritten_chunk_size / (double) destor.data_size);

	if (destor.simulation_level == SIMULATION_NO)
		printf("simulation level is %s\n", "NO");
	else if (destor.simulation_level == SIMULATION_RESTORE)
		printf("simulation level is %s\n", "RESTORE");
	else if (destor.simulation_level == SIMULATION_APPEND)
		printf("simulation level is %s\n", "APPEND");
	else if (destor.simulation_level == SIMULATION_ALL)
		printf("simulation level is %s\n", "ALL");
	else {
		printf("Invalid simulation level.\n");
	}

	printf("=== destor stat ===\n");
	exit(0);
}

int main(int argc, char **argv) {

	destor_start();

	int job = DESTOR_BACKUP;
	int revision = -1;
	sds tiny_dir = sdsnew(".");

	sds path = NULL;
	sds* keywords; // DedupSearch
	size_t keywords_num; // DedupSearch
	size_t keywords_length = 0;
	sds keywords_file = NULL;

	int opt = 0;
	while ((opt = getopt_long(argc, argv, short_options, long_options, NULL))
			!= -1) {
		switch (opt) {
		case 'r':
			job = DESTOR_RESTORE;
			revision = atoi(optarg);
			break;
		case 's':
			destor_stat();
			break;
		case 't':
			job = DESTOR_MAKE_TRACE;
			break;
		case 'h':
			usage();
			break;
		case 'p': {
			sds param = sdsnew(optarg);
			load_config_from_string(param);
			break;
		}
		case 'd': {// DedupSearch physical search, database disk path for tiny
			sdsfree(tiny_dir);
			tiny_dir = sdsnew(argv[optind++]);
			break;
		}
		case 'n': {// DedupSearch naive search
			job = DESTOR_NAIVE_SEARCH;
			revision = atoi(optarg);
			break;
		}
		case 'q': {// DedupSearch quick search
			job = DESTOR_QUICK_SEARCH;
			revision = atoi(optarg);
			break;
		}
		case 'l': {// DedupSearch length of keywords (const)
			keywords_length = atoi(optarg);
			break;
		}
		case 'f': {// DedupSearch keywords file for search
			keywords_file = sdsnew(optarg);
			break;
		}
		default:
			return 0;
		}
	}

	switch (job) {
	case DESTOR_BACKUP:

		if (argc > optind) {
			path = sdsnew(argv[optind]);
		} else {
			fprintf(stderr, "backup job needs a protected path!\n");
			usage();
		}

		do_backup(path);

		/*
		 * The backup concludes.
		 * GC starts
		 * */
		if(destor.backup_retention_time >= 0
				&& jcr.id >= destor.backup_retention_time){
			NOTICE("GC is running!");
			do_delete(jcr.id - destor.backup_retention_time);
		}

		sdsfree(path);

		break;
	case DESTOR_RESTORE:
		if (revision < 0) {
			fprintf(stderr, "A job id is required!\n");
			usage();
		}
		if (argc > optind) {
			path = sdsnew(argv[optind]);
		} else {
			fprintf(stderr, "A target directory is required!\n");
			usage();
		}

		do_restore(revision, path[0] == 0 ? 0 : path);

		sdsfree(path);
		break;
	case DESTOR_MAKE_TRACE: {
		if (argc > optind) {
			path = sdsnew(argv[optind]);
		} else {
			fprintf(stderr, "A target directory is required!\n");
			usage();
		}

		make_trace(path);
		sdsfree(path);
		break;
	}
	case DESTOR_NAIVE_SEARCH: // DedupSearch
		if (revision < 0) {
			fprintf(stderr, "A job id is required!\n");
			usage();
		}
		keywords = get_keywords(keywords_file, &keywords_num, keywords_length, argc, argv);
		
		do_naive_search(revision, keywords, keywords_num);

		for (int i = 0; i < keywords_num; i++, optind++){
			sdsfree(keywords[i]);
		}
		free (keywords);
		sdsfree(path);
		break;
	case DESTOR_QUICK_SEARCH: // DedupSearch
		if (revision < 0) {
			fprintf(stderr, "A job id is required!\n");
			usage();
		}
		keywords = get_keywords(keywords_file, &keywords_num, keywords_length, argc, argv);
		sds tiny_path = sdscat(tiny_dir, "/tiny.db");
		printf("tiny path: %s\n", tiny_path);
		do_quick_search(revision, keywords, keywords_num, tiny_path);

		for (int i = 0; i < keywords_num; i++, optind++){
			sdsfree(keywords[i]);
		}
		free (keywords);
		sdsfree(path);
		sdsfree(tiny_path);
		break;
	default:
		fprintf(stderr, "Invalid job type!\n");
		usage();
	}
	// sdsfree(tiny_dir);
	destor_shutdown();

	return 0;
}

struct chunk* new_chunk(int32_t size) {
	struct chunk* ck = (struct chunk*) malloc(sizeof(struct chunk));

	ck->flag = CHUNK_UNIQUE;
	ck->id = TEMPORARY_ID;
	memset(&ck->fp, 0x0, sizeof(fingerprint));
	ck->size = size;

	if (size > 0)
		ck->data = malloc(size);
	else
		ck->data = NULL;

	return ck;
}

void free_chunk(struct chunk* ck) {
	if (ck->data) {
		free(ck->data);
		ck->data = NULL;
	}
	free(ck);
}

struct segment* new_segment() {
	struct segment * s = (struct segment*) malloc(sizeof(struct segment));
	s->id = TEMPORARY_ID;
	s->chunk_num = 0;
	s->chunks = g_sequence_new(NULL);
	s->features = NULL;
	return s;
}

struct segment* new_segment_full(){
	struct segment* s = new_segment();
	s->features = g_hash_table_new_full(g_feature_hash, g_feature_equal, free, NULL);
	return s;
}

void free_segment(struct segment* s) {
	GSequenceIter *begin = g_sequence_get_begin_iter(s->chunks);
	GSequenceIter *end = g_sequence_get_end_iter(s->chunks);
	for(; begin != end; begin = g_sequence_get_begin_iter(s->chunks)){
		free_chunk(g_sequence_get(begin));
		g_sequence_remove(begin);
	}
	g_sequence_free(s->chunks);

	if (s->features)
		g_hash_table_destroy(s->features);

	free(s);
}

gboolean g_fingerprint_equal(fingerprint* fp1, fingerprint* fp2) {
	return !memcmp(fp1, fp2, sizeof(fingerprint));
}

gint g_fingerprint_cmp(fingerprint* fp1, fingerprint* fp2, gpointer user_data) {
	return memcmp(fp1, fp2, sizeof(fingerprint));
}

gint g_chunk_cmp(struct chunk* a, struct chunk* b, gpointer user_data){
	return memcmp(&a->fp, b->fp, sizeof(fingerprint));
}
