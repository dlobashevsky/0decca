
SHELL=/bin/bash
CC=gcc

ifeq (,$(LOCALDEBUG))
DBGFLAG ?= -O3 -march=native -mtune=native -funroll-loops
else
DBGFLAG=-ggdb3 -DLOCALDEBUG=1
endif

CFLAGS+= -std=gnu17 -D_GNU_SOURCE -D_REENTRANT $(DBGFLAG) -fPIC -Wall -Wno-parentheses -Wno-switch -Wno-pointer-sign -Wno-trampolines -Wno-unused-result


LDFLAGS= -lsqlite3 -luuid -lxxhash -ljansson -lcmph -lrt -lpthread

SRC= $(wildcard *.c)
OBJS= $(SRC:.c=.o)

DFILES= $(SRC:.c=.d)
HFILES= $(wildcard *.h)

PROG=0decca

GOALS= $(PROG)

.PHONY: all test doc docs clean dist install generated

all:  $(GENERATED) $(DFILES) $(GOALS)

generated: $(GENERATED)

doc docs:
	cd .. ; rm -Rf docs/*; doxygen
	make -C ../docs/latex pdf

$(DFILES): $(HFILES)


$(PROG): $(OBJS)
	$(CC) $^ $(LDFLAGS) -o $@


%.d:	%.c
	$(CC) -MM -MG $(CFLAGS) $< > $@

install: $(PROG)
	sudo cp $(PROG) /usr/local/bin

clean:
	rm -fR $(OBJS) $(DFILES) $(PROG) *.test semantic.cache* *.tmp *.tmp~ docs *.inc 0vg*

dist:  clean
#	rm -fR $(GENERATED)

ifeq (,$(findstring $(MAKECMDGOALS),dist clean depend doc docs))
-include $(DFILES)
endif

