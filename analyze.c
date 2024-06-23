#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>

#define MAX_DISTINCT_CITIES 512

struct citydata {
  char *str;
  unsigned len;
  double max;
  double min;
  double sum;
  unsigned count;
};

struct result {
  struct citydata *cities;
  unsigned capacity;
  unsigned length;
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

static inline void insert_name(struct result *result, struct citydata city) {
  void *res = bsearch(&city, result->cities, result->length, sizeof(city),
                      citydata_cmp);
  if (res == NULL) {
    if (result->length >= result->capacity) {
      fprintf(stderr, "too many items for capacity\n");
      abort();
    }
    city.min = city.max;
    city.sum = city.max;
    result->cities[result->length] = city;
    result->length++;
    qsort(result->cities, result->length, sizeof(city), citydata_cmp);
  } else {
    struct citydata *resdata = res;
    if (city.max > resdata->max) {
      resdata->max = city.max;
    }
    if (city.max < resdata->min) {
      resdata->min = city.max;
    }
    resdata->count++;
    resdata->sum += city.max;
  }
}

// Thread target that parses lines
static void *parse_lines(void *arg) {
  struct threadinfo *info = arg;

  char *start = info->start;
  struct result result = info->result;
  enum {
    READING_NAME,
    READING_NUMBER,
    READING_DECIMAL
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
        current_city.max = 0.0;
        //printf("  %.*s;", current_city.len, current_city.str);
      } else {
        current_city.len++;
      }
      break;
    case READING_NUMBER:
      if (c == '.') {
        state = READING_DECIMAL;
      } else if (c == '-') {
        negative_number = true;
      } else {
        current_city.max += c - '0';
        current_city.max *= 10;
      }
      break;
    case READING_DECIMAL:
      if (c == '\n') {
        if (negative_number) {
          current_city.max = -current_city.max;
          negative_number = false;
        }
        state = READING_NAME;
        insert_name(&result, current_city);
        current_city.str = start + i + 1;
        current_city.len = 0;
        //printf("%f\n", current_city.max);
      } else {
        current_city.max += c - '0';
        current_city.max /= 10;
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
  all_cities = malloc(sizeof(*all_cities) * MAX_DISTINCT_CITIES * num_threads);
  for (int i = 0; i < num_threads * MAX_DISTINCT_CITIES; i++) {
    all_cities[i].len = 0;
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
      threads[i].result.cities = all_cities + i * MAX_DISTINCT_CITIES;
      threads[i].result.capacity = MAX_DISTINCT_CITIES;
    }
  }

  // Launch threads, join them
  for (int i = 0; i < num_threads; i++) {
    pthread_create(&threads[i].thread, NULL, parse_lines, (void *)&threads[i]);
  }
  for (int i = 0; i < num_threads; i++) {
    pthread_join(threads[i].thread, NULL);
  }

  // Sort all cities again, merge equal ones
  qsort(all_cities, MAX_DISTINCT_CITIES * num_threads, sizeof(*all_cities), citydata_cmp);
  unsigned long count = 1;
  struct citydata *prev = all_cities;
  for (int i = 1; i < MAX_DISTINCT_CITIES * num_threads; i++) {
    struct citydata *curr = all_cities + i;
    if (curr->len == 0) {
      continue;
    }

    if (citydata_cmp(curr, prev) == 0) {
      if (curr->max > prev->max) {
        prev->max = curr->max;
      }
      if (curr->min < prev->min) {
        prev->min = curr->min;
      }
      prev->sum += curr->sum;
      prev->count += curr->count;
    } else {
      if (prev->len > 0) {
        printf("%.*s max:%.1f min:%.1f avg:%.1f\n", prev->len, prev->str,
               prev->max, prev->min, prev->sum / prev->count);
      }
      count++;
      prev = curr;
    }
  }
  printf("Distinct cities: %lu\n", count);

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
