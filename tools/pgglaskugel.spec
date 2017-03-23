# https://fedoraproject.org/wiki/PackagingDrafts/Go#Build_ID
%global _dwz_low_mem_die_limit 0

Name:           pgglaskugel
Version:        0.4
Release:        1
BuildArch:      x86_64
Summary:        A tool that helps you to manage your PostgreSQL backups
License:        MIT
URL:            https://github.com/xxorde/%{name}

#Source0:       pgglaskugel.tar.gz
# activate this, when the repo is public, and make sure to use spectool -g pgglaskugel.spec befor rpmbuild 
# to get the sources for the release, if it changes
Source0:        https://github.com/xxorde/%{name}/archive/v%{version}.tar.gz

#BuildRequires:  golang >= 1.6

Requires: postgresql, zstd, tar

%description
This is a personal work-in-progress project! Do not expect anything to work as intended jet!
Feel free to send bug reports, use --debug! :)
This should become an easy to use (backup) tool for PostgreSQL.

%prep
%setup -q -n %{name}-%{version}

%build
# *** ERROR: No build ID note found in /.../BUILDROOT/
function gobuild { go build -a -ldflags "-B 0x$(head -c20 /dev/urandom|od -An -tx1|tr -d ' \n')" -v -x "$@"; }

# set up temporary build gopath, and put our directory there, as long as the repo is private
# if the repo is public, we only need the _build-folder to store the sources temporary
mkdir -p ./_build/src/github.com/xxorde/
ln -s $(pwd) ./_build/src/github.com/xxorde/%{name}

# this is important, so gobuild can find the sources in the _build-folder
export GOPATH="$(pwd)/_build"

go get -u github.com/golang/dep/...
dep ensure

# activate this, when the repo is public
#go get github.com/xxorde/%{name}

gobuild -o %{name} .

%install
install -d %{buildroot}%{_bindir}
install -p -m 0755 ./%{name} %{buildroot}%{_bindir}/%{name}

%files
%defattr(-,root,root,-)
%doc README.md
%{_bindir}/%{name}

%changelog
* Thu Mar  9 2017 Danilo Endesfelder <danilo.endesfelder@credativ.de>
  Initial package version.
