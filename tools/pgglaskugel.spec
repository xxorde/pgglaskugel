Name:           pgglaskugel
Version:        0.5
Release:        1
BuildArch:      x86_64
Summary:        A tool that helps you to manage your PostgreSQL backups
License:        MIT
URL:            https://github.com/xxorde/%{name}
Source0:        https://circleci.com/api/v1/project/xxorde/pgglaskugel/latest/artifacts/0/$CIRCLE_ARTIFACTS/pgGlaskugel.tar.xz
Requires: postgresql, gpg, tar, zstd

%description
This is a personal work-in-progress project! Do not expect anything to work as intended jet!
Feel free to send bug reports, use --debug! :)
This should become an easy to use (backup) tool for PostgreSQL.

%prep
%setup -c %{name}-%{version}

#%install
install -d %{buildroot}/%{_bindir}/
install -m 755 %{name} %{buildroot}/%{_bindir}/

%files
%defattr(-,root,root,-)
%doc docs
%{_bindir}/%{name}

%clean
rm -rf %{buildroot}

%changelog
* Thu Mar  23 2017 Alexander Sosna <alexander@xxor.de>
  Initial package version.
