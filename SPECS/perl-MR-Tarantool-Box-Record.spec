# build type is debug or release

Name:           perl-MR-Tarantool-Box-Record
Version:        %{__version}
Release:        1%{?dist}

Summary:        ActiveRecord for a tuple of Tarantool/Octopus
License:        BSD
Group:          MAILRU

BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-buildroot
BuildArch:      noarch
Requires:       perl-Mouse
Requires:       perl-MR-IProto-XS >= 20130719.1722
Requires:       perl-MR-Tarantool-Box-XS >= 20130719.1722
Requires:       perl-List-MoreUtils

%description
ActiveRecord for a tuple of Tarantool/Octopus. Built from revision %{__gitrelease}.

%prep
%setup -n iproto/tarantool/record

%build
%{__perl} Makefile.PL INSTALLDIRS=vendor
make %{?_smp_mflags}

%install
[ "%{buildroot}" != "/" ] && rm -fr %{buildroot}
make pure_install PERL_INSTALL_ROOT=$RPM_BUILD_ROOT
find $RPM_BUILD_ROOT -type f -name .packlist -exec rm -f {} ';'
find $RPM_BUILD_ROOT -depth -type d -exec rmdir {} 2>/dev/null ';'
chmod -R u+w $RPM_BUILD_ROOT/*

%files
%defattr(-,root,root,-)
%{perl_vendorlib}/*

%changelog
* Tue Jul 2 2013 Aleksey Mashanov <a.mashanov@corp.mail.ru>
- initial version
