package MR::Tarantool::Box::Record;

use Mouse ();
use Mouse::Exporter;
use Mouse::Util::MetaRole;
use MR::IProto::XS;

Mouse::Exporter->setup_import_methods(
    as_is => [ 'iproto', 'namespace', 'has_field' ],
    also  => 'Mouse',
);

sub init_meta {
    my ($class, %args) = @_;
    Mouse->init_meta(base_class => 'MR::Tarantool::Box::Record::Object', %args);
    Mouse::Util::MetaRole::apply_metaroles(
        for => $args{for_class},
        class_metaroles => {
            class => ['MR::Tarantool::Box::Record::Trait::Class'],
        },
    );
    return $args{for_class}->meta();
}

sub iproto {
    my $iproto = @_ == 1 ? shift : MR::IProto::XS->new(@_);
    caller->meta->iproto($iproto);
}

sub namespace {
    caller->meta->namespace(@_);
}

sub has_field {
    caller->meta->add_field(@_);
}

1;
