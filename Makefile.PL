use ExtUtils::MakeMaker;

WriteMakefile(
    NAME      => 'MR::Tarantool::Box::Record',
    MAKEFILE  => 'Makefile',
    PREREQ_PM => {
        'Mouse'                  => 0,
        'MR::IProto::XS'         => 0,
        'MR::Tarantool::Box::XS' => 0,
    },
);
