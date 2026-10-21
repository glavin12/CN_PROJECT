/*
 * unit.h - minimal unit-test macros (no external framework).
 *
 *   CHECK(cond)            record a failure if cond is false, keep going
 *   CHECK_EQ(a, b)         integer equality, prints both values
 *   CHECK_STR(a, b)        string equality
 *   UNIT_DONE()            print a summary; return from main with 0 / 1
 */
#ifndef FDS_UNIT_H
#define FDS_UNIT_H

#include <stdio.h>
#include <string.h>

static int unit_checks, unit_failures;

#define CHECK(cond) do {                                                       \
        unit_checks++;                                                         \
        if (!(cond)) {                                                         \
            unit_failures++;                                                   \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
        }                                                                      \
    } while (0)

#define CHECK_EQ(a, b) do {                                                    \
        long long unit_a = (long long)(a), unit_b = (long long)(b);            \
        unit_checks++;                                                         \
        if (unit_a != unit_b) {                                                \
            unit_failures++;                                                   \
            fprintf(stderr, "%s:%d: CHECK_EQ failed: %s == %lld, %s == %lld\n", \
                    __FILE__, __LINE__, #a, unit_a, #b, unit_b);               \
        }                                                                      \
    } while (0)

#define CHECK_STR(a, b) do {                                                   \
        const char *unit_sa = (a), *unit_sb = (b);                             \
        unit_checks++;                                                         \
        if (unit_sa == NULL || unit_sb == NULL || strcmp(unit_sa, unit_sb) != 0) { \
            unit_failures++;                                                   \
            fprintf(stderr, "%s:%d: CHECK_STR failed: \"%s\" vs \"%s\"\n",     \
                    __FILE__, __LINE__, unit_sa ? unit_sa : "(null)",          \
                    unit_sb ? unit_sb : "(null)");                             \
        }                                                                      \
    } while (0)

#define UNIT_DONE() do {                                                       \
        printf("%s: %d checks, %d failures\n", __FILE__, unit_checks, unit_failures); \
        return unit_failures ? 1 : 0;                                          \
    } while (0)

#endif /* FDS_UNIT_H */
