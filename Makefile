NAME = pgglaskugel
PACKAGE = github.com/xxorde/$(NAME)
VERSION = 0.5
BUILD_TIME = $(shell date +%FT%T%z)
LDFLAGS = -ldflags "-X $(PACKAGE)/cmd.Version=$(VERSION) -X $(PACKAGE)/cmd.Buildtime=$(BUILD_TIME)"

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
	go build -race $(LDFLAGS) -o $(NAME)

man:
	./$(NAME) genman

test:
	go test -v -race

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
