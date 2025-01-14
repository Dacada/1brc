#include <emmintrin.h>
#include <immintrin.h>
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
#define HASH_SEED_1 0xb00b1350
#define HASH_SEED_2 0xcafebeef

struct stringslice {
  char *str;
  unsigned len;
};

struct cityline {
  struct stringslice str;
  int measure;
};

struct citydata {
  struct stringslice str;
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

__attribute__((pure))
static int stringslice_cmp(const void *a, const void *b) {
  const struct stringslice *aa = a;
  const struct stringslice *bb = b;

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
    current_city = city;
  } else {
    if (stringslice_cmp(&city.str, &current_city.str) != 0) {
      return false;
    }
    if (city.max > current_city.max) {
      current_city.max = city.max;
    }
    if (city.min < current_city.min) {
      current_city.min = city.min;
    }
    current_city.count += city.count;
    current_city.sum += city.sum;
  }
  result->cities[hash] = current_city;
  return true;
}

static inline void insert_name(struct result *result, struct citydata city) {
  unsigned hash1 = HASH_SEED_1;
  unsigned hash2 = HASH_SEED_2;
  hashlittle2(city.str.str, city.str.len, &hash1, &hash2);

  for (int i = 0; i < HASHTABLE_SIZE; i++) {
    unsigned h1 = (hash1 + i) & (HASHTABLE_SIZE - 1);
    unsigned h2 = (hash2 + i) & (HASHTABLE_SIZE - 1);
    if (insert_name_hashed(result, city, h1) ||
        insert_name_hashed(result, city, h2)) {
      return;
    }
  }

  fprintf(stderr, "hashtable full\n");
  abort();
}

// ASSUMPTIONS: c is always present in str and str is allocated such that there are at least 16 bytes after the
// appearence of c
__attribute__((pure))
static inline unsigned find_character(const char *str, char c) {
  unsigned i = 0;
  int mask = 0;
  __m128i target = _mm_set1_epi8(c);

  while (!mask) {
    __m128i chunk = _mm_loadu_si128((__m128i *)(str + i));
    __m128i res = _mm_cmpeq_epi8(chunk, target);
    mask = _mm_movemask_epi8(res);
    i += 16;
  }

  return __builtin_ffs(mask) - 1 + i - 16;
}

// Parse a single line
static inline int parse_line(char *str, struct cityline *city) {
  city->str.str = str;

  unsigned i;

  i = find_character(str, ';');
  city->str.len = i;
  i++;

  bool neg = str[i] == '-';
  if (neg) {
    i++;
  }

  int n = 0;
  for (;; i++) {
    char c = str[i];
    if (c == '.') {
      break;
    }
    n *= 10;
    n += c - '0';
  }
  i++;

  n *= 10;
  n += str[i] - '0';
  i++; // \n
  i++;

  if (neg) {
    n = -n;
  }
  city->measure = n;

  return i;
}

// Thread target that parses lines
static void *parse_lines(void *arg) {
  struct threadinfo *info = arg;

  char *start = info->start;
  struct result result = info->result;
  struct cityline current_city;

  unsigned long i = 0;
  while (i < info->size) {
    int ii = parse_line(start + i, &current_city);
    /* printf("%.*s;%d\n", current_city.str.len, current_city.str.str, */
    /*        current_city.measure); */
    struct citydata new_city;
    new_city.count = 1;
    new_city.max = current_city.measure;
    new_city.min = current_city.measure;
    new_city.sum = current_city.measure;
    new_city.str = current_city.str;
    insert_name(&result, new_city);
    i += ii;
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

  // Map the file into memory, we map it 16 bytes larger to ensure SIMD instructions will not read out of bounds.
  mapped = mmap(NULL, sb.st_size + 16, PROT_READ, MAP_PRIVATE, fd, 0);
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

  // Or run them sequentially for testing
  /* for (int i = 0; i < num_threads; i++) { */
  /*   parse_lines(&threads[i]); */
  /* } */

  // Merge all hash tables into the first
  for (int i = 1; i < num_threads; i++) {
    for (int j = 0; j < HASHTABLE_SIZE; j++) {
      struct citydata city = threads[i].result.cities[j];
      if (city.count > 0) {
        insert_name(&threads[0].result, city);
      }
    }
  }

  // Treat the first hash table as a list and sort it
  qsort(threads[0].result.cities, HASHTABLE_SIZE, sizeof(*all_cities),
        stringslice_cmp);

  // Output the results -- Not the exact correct output format but I'm not dealing with that
  for (int i = 0; i < HASHTABLE_SIZE; i++) {
    struct citydata city = threads[0].result.cities[i];
    if (city.count > 0) {
      printf("%.*s=%.1f/%.1f/%.1f\n", city.str.len, city.str.str,
             (double)city.max / 10.0, (double)city.min / 10.0,
             (double)city.sum / (double)city.count / 10.0);
    }
  }

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
