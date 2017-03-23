NAME := pgglaskugel
BUILD := "_build"
INSTALL := /

BIN := /usr/bin
SHARE := /usr/share/$(NAME)
ARCHIVE_NAME := pgGlaskugel.tar.xz

.PHONY: all test $(NAME) clean 

all: $(NAME) 

$(NAME):
	go build -o $(NAME)

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
	rm -rf $(BUILD)
	rm -rf $(NAME) 
	rm -rf *.tar*
	rm -rf *.rpm
	rm -rf *.dep
