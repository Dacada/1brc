#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>

// hashing functions
#include "lookup3.c"

#define HASHTABLE_SIZE (1 << 12)
#define HASH_SEED_1 0xb00b135f
#define HASH_SEED_2 0xcafebeef

struct citydata {
  char *str;
  unsigned len;
  int max;
  int min;
  long sum;
  unsigned long count;
};

struct result {
  struct citydata *cities;
};

struct threadinfo {
  pthread_t thread;
  char *start;
  unsigned long size;
  struct result result;
};

static int citydata_cmp(const void *a, const void *b) {
  const struct citydata *aa = a;
  const struct citydata *bb = b;

  for (unsigned i = 0;; i++) {
    bool as = aa->len <= i;
    bool bs = bb->len <= i;
    if (as && bs) {
      return 0;
    }
    if (as) {
      return -1;
    }
    if (bs) {
      return 1;
    }
    char ac = aa->str[i];
    char bc = bb->str[i];
    if (ac < bc) {
      return -1;
    }
    if (ac > bc) {
      return 1;
    }
  }
}

static inline bool insert_name_hashed(struct result *result,
                                      struct citydata city, unsigned hash) {
  struct citydata current_city = result->cities[hash];
  if (current_city.count == 0) {
    city.min = city.max;
    city.sum = city.max;
    city.count = 1;
    current_city = city;
  } else {
    if (citydata_cmp(&city, &current_city) != 0) {
      return false;
    }
    if (city.max > current_city.max) {
      current_city.max = city.max;
    }
    if (city.min < current_city.max) {
      current_city.min = city.max;
    }
    current_city.count++;
    current_city.sum += city.max;
  }
  result->cities[hash] = current_city;
  return true;
}

static int greatest_hash_i = 0;
static inline void insert_name(struct result *result, struct citydata city) {
  unsigned hash1 = HASH_SEED_1;
  unsigned hash2 = HASH_SEED_2;
  hashlittle2(city.str, city.len, &hash1, &hash2);

  for (int i = 0; i < HASHTABLE_SIZE; i++) {
    unsigned h1 = (hash1 + i) & (HASHTABLE_SIZE - 1);
    unsigned h2 = (hash2 + i) & (HASHTABLE_SIZE - 1);
    if (insert_name_hashed(result, city, h1) ||
        insert_name_hashed(result, city, h2)) {
      if (i > greatest_hash_i) {
        greatest_hash_i = i;
      }
      return;
    }
  }

  fprintf(stderr, "hashtable full\n");
  abort();
}

// Thread target that parses lines
static void *parse_lines(void *arg) {
  struct threadinfo *info = arg;

  char *start = info->start;
  struct result result = info->result;
  enum {
    READING_NAME,
    READING_NUMBER,
  } state = READING_NAME;
  struct citydata current_city;
  current_city.len = 0;
  current_city.str = start;
  bool negative_number = false;

  for (unsigned long i = 0; i < info->size; i++) {
    char c = start[i];
    switch (state) {
    case READING_NAME:
      if (c == ';') {
        state = READING_NUMBER;
        current_city.max = 0;  // use max to carry the parsed measurement instead of copying it everywhere
        // printf("  %.*s;", current_city.len, current_city.str);
      } else {
        current_city.len++;
      }
      break;
    case READING_NUMBER:
      if (c == '.') {
      } else if (c == '-') {
        negative_number = true;
      } else if (c == '\n') {
        if (negative_number) {
          current_city.max = -current_city.max;
          negative_number = false;
        }
        state = READING_NAME;
        // printf("%d\n", current_city.max);
        insert_name(&result, current_city);
        current_city.str = start + i + 1;
        current_city.len = 0;
      } else {
        current_city.max *= 10;
        current_city.max += c - '0';
      }
      break;
    }
  }
  info->result = result;

  return NULL;
}

int main(int argc, char *argv[]) {
  int fd;
  struct stat sb;
  char *mapped;
  char *filename;
  int num_threads;
  unsigned long thread_workload;
  struct threadinfo *threads;
  struct citydata *all_cities;

  // Get filename from arguments
  if (argc != 2) {
    fprintf(stderr, "Usage: %s <filename>\n", argv[0]);
    exit(EXIT_FAILURE);
  }
  filename = argv[1];

  // Open the file
  fd = open(filename, O_RDONLY);
  if (fd == -1) {
    perror("open");
    exit(EXIT_FAILURE);
  }

  // Get the file info (size is all we really care about)
  if (fstat(fd, &sb) == -1) {
    perror("fstat");
    exit(EXIT_FAILURE);
  }

  // Map the file into memory
  mapped = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
  if (mapped == MAP_FAILED) {
    perror("mmap");
    exit(EXIT_FAILURE);
  }

  // Provide a hint to the kernel about the expected access pattern
  if (madvise(mapped, sb.st_size, MADV_SEQUENTIAL) == -1) {
    perror("madvise");
    exit(EXIT_FAILURE);
  }

  // Create thread information
  num_threads = sysconf(_SC_NPROCESSORS_ONLN);
  threads = malloc(sizeof(*threads) * num_threads);
  thread_workload = sb.st_size / num_threads;

  // Reserve memory used by all threads
  all_cities = malloc(sizeof(*all_cities) * HASHTABLE_SIZE * num_threads);
  for (int i = 0; i < num_threads * HASHTABLE_SIZE; i++) {
    all_cities[i].count = 0;
  }

  // Initialize threads
  {
    unsigned long total_workload = 0;
    for (int i = 0; i < num_threads; i++) {
      threads[i].start = mapped + total_workload;
      unsigned workload = thread_workload;
      if (i == num_threads - 1) {
        workload = sb.st_size - total_workload;
      } else {
        while (threads[i].start[workload-1] != '\n') {
          workload--;
        }
      }
      threads[i].size = workload;
      total_workload += workload;
      threads[i].result.cities = all_cities + i * HASHTABLE_SIZE;
    }
  }

  // Launch threads, join them
  for (int i = 0; i < num_threads; i++) {
    pthread_create(&threads[i].thread, NULL, parse_lines, (void *)&threads[i]);
  }
  for (int i = 0; i < num_threads; i++) {
    pthread_join(threads[i].thread, NULL);
  }
  /* for (int i = 0; i < num_threads; i++) { */
  /*   parse_lines(&threads[i]); */
  /* } */

  // Merge all results into one and sort it
  for (int i = 1; i < num_threads; i++) {
    for (int j = 0; j < HASHTABLE_SIZE; j++) {
      struct citydata city = threads[i].result.cities[j];
      if (city.count > 0) {
        insert_name(&threads[0].result, city);
      }
    }
  }
  qsort(threads[0].result.cities, HASHTABLE_SIZE, sizeof(*all_cities),
        citydata_cmp);
  unsigned long count = 0;
  for (int i = 0; i < HASHTABLE_SIZE; i++) {
    struct citydata city = threads[0].result.cities[i];
    if (city.count > 0) {
      count++;
      printf("%.*s max:%.1f min:%.1f avg:%.1f\n", city.len, city.str,
               (double)city.max / 10.0, (double)city.min / 10.0, (double)city.sum / (double)city.count / 10.0);
    }
  }
  printf("Distinct cities: %lu\n", count);
  printf("Greatest hash: %d\n", greatest_hash_i);

  // Unmap the file
  if (munmap(mapped, sb.st_size) == -1) {
    perror("munmap");
    exit(EXIT_FAILURE);
  }

  // Close the file descriptor
  close(fd);

  // Free memory
  free(threads);
  free(all_cities);

  return 0;
}
