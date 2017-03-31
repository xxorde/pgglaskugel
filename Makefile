NAME = pgglaskugel
PACKAGE = github.com/xxorde/$(NAME)
VERSION = $(shell git describe --tags --abbrev=0 | sed "s/^v//")
GITHASH = $(shell git rev-parse HEAD)
LDFLAGS = -ldflags "-X $(PACKAGE)/cmd.Version=$(VERSION) -X $(PACKAGE)/cmd.GitHash=$(GITHASH)"

BUILD = _build
INSTALL = /

BIN = /usr/bin
SHARE = /usr/share/$(NAME)
ARCHIVE_NAME = pgGlaskugel.tar.xz

.PHONY: all vendor test $(NAME) man clean 

all: vendor $(NAME) test man tarball

vendor:
	go get -u github.com/golang/dep/...
	dep ensure

$(NAME):
	echo $(LDFLAGS)
	go build -race $(LDFLAGS) -o $(NAME)

man:
	./$(NAME) genman

test:
	go test -v -race

testsuite:
	cd tools/Test-CentOS7; ./run_test_in_docker.sh

tarball:
	mkdir -p $(BUILD)/docs
	install -m 755 $(NAME) $(BUILD)
	install -m 644 README.md LICENSE $(BUILD)/docs
	cp -r docs/* $(BUILD)/docs
	tar cfJ $(ARCHIVE_NAME) -C $(BUILD) .

install:
	mkdir -p  $(INSTALL)/$(SHARE)
	install -m 755 $(NAME) $(INSTALL)/$(BIN) 
	install -m 644 README.md LICENSE $(INSTALL)/$(SHARE)
	cp -r docs $(INSTALL)/$(SHARE)

clean:
	go clean
	rm -rf $(BUILD)
	rm -rf $(NAME) 
	rm -rf *.tar*
	rm -rf *.rpm
	rm -rf *.dep
