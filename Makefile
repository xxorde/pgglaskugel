NAME = pgglaskugel
PACKAGE = github.com/xxorde/$(NAME)
VERSION = 0.5
BUILD_TIME = $(shell date +%FT%T%z)
LDFLAGS = -ldflags "-X $(PACKAGE)/cmd.Version=$(VERSION) -X $(PACKAGE)/cmd.Buildtime=$(BUILD_TIME)"

BUILD = "_build"
INSTALL = /

BIN = /usr/bin
SHARE = /usr/share/$(NAME)
ARCHIVE_NAME = pgGlaskugel.tar.xz

.PHONY: all vendor test $(NAME) clean 

all: vendor $(NAME) test tarball

vendor:
	go get -u github.com/golang/dep/...
	dep ensure

$(NAME):
	go build -race $(LDFLAGS) -o $(NAME)

test:
	go test -v -race

tarball:
	mkdir -p $(BUILD)
	mkdir -p $(BUILD)/$(SHARE)
	mkdir -p $(BUILD)/$(BIN)
	install -m 755 $(NAME) $(BUILD)/$(BIN)	
	install -m 644 README.md LICENSE $(BUILD)/$(SHARE)
	cp -r docs $(BUILD)/$(SHARE)
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
