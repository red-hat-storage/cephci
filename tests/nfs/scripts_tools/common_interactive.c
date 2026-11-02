/*
 * Interactive NFS open/lock helper for TSM grace conflict scenarios.
 *
 * Usage: common_interactive [base_prefix]
 *   base_prefix defaults to /mnt/mani/f → files base0.txt .. base4.txt
 *   Example: common_interactive /mnt/nfs_tsmg/f → /mnt/nfs_tsmg/f0.txt ..
 *
 * Commands (stdin):
 *   open   <index> <mode>     mode: r | w | rw | rwc
 *   rlock / wlock / nrlock / nwlock <index>
 *   getlk  <index> <r|w>
 *   unlock <index>
 *   close  <index>
 *   status
 *   quit
 */
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdbool.h>
#include <stdlib.h>

#define NUM_FILES 5
#define RANGE_SIZE 100

static const char *mode_name(int flags)
{
    if (flags < 0)
        return "-";
    if ((flags & O_ACCMODE) == O_RDONLY)
        return "RDONLY";
    if ((flags & O_ACCMODE) == O_WRONLY)
        return "WRONLY";
    return "RDWR";
}

static const char *ltype_name(short t)
{
    if (t == F_RDLCK)
        return "READ";
    if (t == F_WRLCK)
        return "WRITE";
    return "NONE";
}

int main(int argc, char **argv)
{
    const char *base = (argc > 1 && argv[1] && argv[1][0]) ? argv[1] : "/mnt/mani/f";
    int fds[NUM_FILES];
    int open_flags[NUM_FILES];
    char filenames[NUM_FILES][128];
    struct flock locks[NUM_FILES];
    bool lock_held[NUM_FILES];
    short lock_type[NUM_FILES];

    for (int i = 0; i < NUM_FILES; i++) {
        snprintf(filenames[i], sizeof(filenames[i]), "%s%d.txt", base, i);
        fds[i] = -1;
        open_flags[i] = -1;
        lock_held[i] = false;
        lock_type[i] = F_UNLCK;

        memset(&locks[i], 0, sizeof(struct flock));
        locks[i].l_whence = SEEK_SET;
        locks[i].l_start = i * RANGE_SIZE;
        locks[i].l_len = RANGE_SIZE;
    }

    printf("=== Interactive NFS lock test (all open + all lock kinds) ===\n");
    printf("base=%s\n", base);
    printf("Commands:\n");
    printf("  open   <index> <mode>     mode: r | w | rw | rwc\n");
    printf("  rlock  <index>            blocking   F_RDLCK  (F_SETLKW)\n");
    printf("  wlock  <index>            blocking   F_WRLCK  (F_SETLKW)\n");
    printf("  nrlock <index>            nonblock   F_RDLCK  (F_SETLK)\n");
    printf("  nwlock <index>            nonblock   F_WRLCK  (F_SETLK)\n");
    printf("  getlk  <index> <r|w>      query conflicting lock (F_GETLK)\n");
    printf("  unlock <index>\n");
    printf("  close  <index>\n");
    printf("  status\n");
    printf("  quit\n");
    fflush(stdout);

    while (1) {
        char cmd[32];
        int idx;

        printf("> ");
        fflush(stdout);

        if (scanf("%31s", cmd) != 1)
            continue;

        if (strcmp(cmd, "quit") == 0) {
            printf("Exiting...\n");
            fflush(stdout);
            break;
        }

        if (strcmp(cmd, "status") == 0) {
            printf("\nCurrent state:\n");
            for (int i = 0; i < NUM_FILES; i++) {
                printf("Index %d -> %-20s | ", i, filenames[i]);
                if (fds[i] < 0) {
                    printf("CLOSED\n");
                } else if (lock_held[i]) {
                    printf("OPEN %s fd=%d + %s LOCK [%ld-%ld]\n",
                           mode_name(open_flags[i]), fds[i],
                           ltype_name(lock_type[i]),
                           locks[i].l_start,
                           locks[i].l_start + locks[i].l_len - 1);
                } else {
                    printf("OPEN %s fd=%d (unlocked)\n",
                           mode_name(open_flags[i]), fds[i]);
                }
            }
            printf("\n");
            fflush(stdout);
            continue;
        }

        if (scanf("%d", &idx) != 1 || idx < 0 || idx >= NUM_FILES) {
            printf("Invalid index\n");
            fflush(stdout);
            continue;
        }

        if (strcmp(cmd, "open") == 0) {
            char mode[8];

            if (scanf("%7s", mode) != 1) {
                printf("Usage: open <index> r|w|rw|rwc\n");
                fflush(stdout);
                continue;
            }
            if (fds[idx] >= 0) {
                printf("Index %d already open\n", idx);
                fflush(stdout);
                continue;
            }

            int flags;
            mode_t perm = 0644;
            bool creat = false;

            if (strcmp(mode, "r") == 0) {
                flags = O_RDONLY;
            } else if (strcmp(mode, "w") == 0) {
                flags = O_WRONLY;
            } else if (strcmp(mode, "rw") == 0) {
                flags = O_RDWR;
            } else if (strcmp(mode, "rwc") == 0) {
                flags = O_RDWR | O_CREAT;
                creat = true;
            } else {
                printf("Unknown mode '%s' (use r|w|rw|rwc)\n", mode);
                fflush(stdout);
                continue;
            }

            fds[idx] = creat
                ? open(filenames[idx], flags, perm)
                : open(filenames[idx], flags);

            if (fds[idx] < 0) {
                perror("open");
            } else {
                open_flags[idx] = flags;
                printf("Opened index %d (%s) %s, fd=%d\n",
                       idx, filenames[idx], mode_name(flags), fds[idx]);
            }
            fflush(stdout);
            continue;
        }

        if (strcmp(cmd, "rlock") == 0 || strcmp(cmd, "wlock") == 0 ||
            strcmp(cmd, "nrlock") == 0 || strcmp(cmd, "nwlock") == 0) {

            short want = (cmd[0] == 'n')
                ? (cmd[1] == 'w' ? F_WRLCK : F_RDLCK)
                : (cmd[0] == 'w' ? F_WRLCK : F_RDLCK);
            int cmd_op = (cmd[0] == 'n') ? F_SETLK : F_SETLKW;
            const char *name = (want == F_WRLCK) ? "WRITE" : "READ";
            const char *how = (cmd_op == F_SETLKW) ? "blocking" : "nonblock";

            usleep(400);

            if (fds[idx] < 0) {
                printf("Index %d not open\n", idx);
                fflush(stdout);
                continue;
            }
            if (lock_held[idx] && lock_type[idx] == want) {
                printf("Index %d already has %s lock\n", idx, name);
                fflush(stdout);
                continue;
            }

            locks[idx].l_type = want;
            if (fcntl(fds[idx], cmd_op, &locks[idx]) < 0) {
                perror("lock failed");
            } else {
                lock_held[idx] = true;
                lock_type[idx] = want;
                printf("%s %s locked index %d [%ld-%ld]\n",
                       how, name, idx,
                       locks[idx].l_start,
                       locks[idx].l_start + locks[idx].l_len - 1);
            }
            fflush(stdout);
            continue;
        }

        if (strcmp(cmd, "getlk") == 0) {
            char kind[8];
            struct flock q;

            if (scanf("%7s", kind) != 1) {
                printf("Usage: getlk <index> r|w\n");
                fflush(stdout);
                continue;
            }
            if (fds[idx] < 0) {
                printf("Index %d not open\n", idx);
                fflush(stdout);
                continue;
            }

            memcpy(&q, &locks[idx], sizeof(q));
            if (strcmp(kind, "r") == 0)
                q.l_type = F_RDLCK;
            else if (strcmp(kind, "w") == 0)
                q.l_type = F_WRLCK;
            else {
                printf("Usage: getlk <index> r|w\n");
                fflush(stdout);
                continue;
            }

            if (fcntl(fds[idx], F_GETLK, &q) < 0) {
                perror("getlk failed");
            } else if (q.l_type == F_UNLCK) {
                printf("getlk: range [%ld-%ld] is free for %s lock\n",
                       locks[idx].l_start,
                       locks[idx].l_start + locks[idx].l_len - 1,
                       strcmp(kind, "w") == 0 ? "WRITE" : "READ");
            } else {
                printf("getlk: blocked by pid %d (%s lock) [%ld-%ld]\n",
                       (int)q.l_pid,
                       q.l_type == F_WRLCK ? "WRITE" : "READ",
                       q.l_start,
                       q.l_len ? q.l_start + q.l_len - 1 : -1);
            }
            fflush(stdout);
            continue;
        }

        if (strcmp(cmd, "unlock") == 0) {
            if (fds[idx] < 0 || !lock_held[idx]) {
                printf("Index %d not locked\n", idx);
                fflush(stdout);
                continue;
            }

            struct flock unlock = locks[idx];
            unlock.l_type = F_UNLCK;

            if (fcntl(fds[idx], F_SETLK, &unlock) < 0) {
                perror("unlock failed");
            } else {
                lock_held[idx] = false;
                lock_type[idx] = F_UNLCK;
                printf("Unlocked index %d\n", idx);
            }
            fflush(stdout);
            continue;
        }

        if (strcmp(cmd, "close") == 0) {
            if (fds[idx] < 0) {
                printf("Index %d already closed\n", idx);
                fflush(stdout);
                continue;
            }

            if (lock_held[idx]) {
                struct flock unlock = locks[idx];
                unlock.l_type = F_UNLCK;
                fcntl(fds[idx], F_SETLK, &unlock);
                lock_held[idx] = false;
                lock_type[idx] = F_UNLCK;
            }

            close(fds[idx]);
            fds[idx] = -1;
            open_flags[idx] = -1;
            printf("Closed index %d\n", idx);
            fflush(stdout);
            continue;
        }

        printf("Unknown command: %s\n", cmd);
        fflush(stdout);
    }

    for (int i = 0; i < NUM_FILES; i++) {
        if (fds[i] >= 0) {
            if (lock_held[i]) {
                struct flock unlock = locks[i];
                unlock.l_type = F_UNLCK;
                fcntl(fds[i], F_SETLK, &unlock);
            }
            close(fds[i]);
        }
    }

    return 0;
}
