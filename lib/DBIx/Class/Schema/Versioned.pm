package DBIx::Class::Version::Table;
use base 'DBIx::Class';
use strict;
use warnings;

__PACKAGE__->load_components(qw/ Core/);
__PACKAGE__->table('SchemaVersions');

__PACKAGE__->add_columns
    ( 'Version' => {
        'data_type' => 'VARCHAR',
        'is_auto_increment' => 0,
        'default_value' => undef,
        'is_foreign_key' => 0,
        'name' => 'Version',
        'is_nullable' => 0,
        'size' => '10'
        },
      'Installed' => {
          'data_type' => 'VARCHAR',
          'is_auto_increment' => 0,
          'default_value' => undef,
          'is_foreign_key' => 0,
          'name' => 'Installed',
          'is_nullable' => 0,
          'size' => '20'
          },
      );
__PACKAGE__->set_primary_key('Version');

package DBIx::Class::Version;
use base 'DBIx::Class::Schema';
use strict;
use warnings;

__PACKAGE__->register_class('Table', 'DBIx::Class::Version::Table');


# ---------------------------------------------------------------------------
package DBIx::Class::Versioning;

use strict;
use warnings;
use base 'DBIx::Class';
use POSIX 'strftime';
use Data::Dumper;
# use DBIx::Class::Version;

__PACKAGE__->mk_classdata('_filedata');
__PACKAGE__->mk_classdata('upgrade_directory');

sub on_connect
{
    my ($self) = @_;
#    print "on_connect\n";
    my $vschema = DBIx::Class::Version->connect(@{$self->storage->connect_info()});
    my $vtable = $vschema->resultset('Table');
    my $pversion;
    if(!$self->exists($vtable))
    {
#        print "deploying.. \n";
        $vschema->storage->debug(1);
#        print "Debugging is: ", $vschema->storage->debug, "\n";
        $vschema->deploy();
        $pversion = 0;
    }
    else
    {
        my $psearch = $vtable->search(undef, 
                                      { select => [
                                                   { 'max' => 'Installed' },
                                                   ],
                                            as => ['maxinstall'],
                                        })->first;
        $pversion = $vtable->search({ Installed => $psearch->get_column('maxinstall'),
                                  })->first;
        $pversion = $pversion->Version if($pversion);
    }
#    warn("Previous version: $pversion\n");
    if($pversion eq $self->VERSION)
    {
        warn "This version is already installed\n";
        return 1;
    }

    
    $vtable->create({ Version => $self->VERSION,
                      Installed => strftime("%Y-%m-%d %H:%M:%S", gmtime())
                      });

    if(!$pversion)
    {
        warn "No previous version found, skipping upgrade\n";
        return 1;
    }

#    $self->create_upgrades($self->upgrade_directoy, $pversion, $self->VERSION);

    my $file = $self->ddl_filename($self->upgrade_directory,
                                   $self->storage->sqlt_type,
                                   $self->VERSION
                                   );
    if(!$file)
    {
        # No upgrade path between these two versions
        return 1;
    }

    $file =~ s/@{[ $self->VERSION ]}/"${pversion}-" . $self->VERSION/e;
    if(!-f $file)
    {
        warn "Upgrade not possible, no upgrade file found ($file)\n";
        return;
    }
#    print "Found Upgrade file: $file\n";
    my $fh;
    open $fh, "<$file" or warn("Can't open upgrade file, $file ($!)");
    my @data = split(/;\n/, join('', <$fh>));
    close($fh);
    @data = grep { $_ && $_ !~ /^-- / } @data;
    @data = grep { $_ !~ /^(BEGIN TRANACTION|COMMIT)/m } @data;
#    print "Commands: ", join("\n", @data), "\n";
    $self->_filedata(\@data);

    $self->backup();
    $self->upgrade();

# X Create version table if not exists?
# Make backup
# Run create statements
# Run post-create callback
# Run alter/drop statement
# Run post-alter callback
}

sub exists
{
    my ($self, $rs) = @_;

    eval {
        $rs->search({ 1, 0 })->count;
    };

    return 0 if $@;

    return 1;
}

sub backup
{
    my ($self) = @_;
    ## Make each ::DBI::Foo do this
#    $self->storage->backup();
}

sub upgrade
{
    my ($self) = @_;

    ## overridable sub, per default just run all the commands.

    $self->run_upgrade(qr/create/i);
    $self->run_upgrade(qr/alter table .*? add/i);
    $self->run_upgrade(qr/alter table .*? (?!drop)/i);
    $self->run_upgrade(qr/alter table .*? drop/i);
    $self->run_upgrade(qr/drop/i);
    $self->run_upgrade(qr//i);
}


sub run_upgrade
{
    my ($self, $stm) = @_;
#    print "Reg: $stm\n";
    my @statements = grep { $_ =~ $stm } @{$self->_filedata};
#    print "Statements: ", join("\n", @statements), "\n";
    $self->_filedata([ grep { $_ !~ /$stm/i } @{$self->_filedata} ]);

    for (@statements)
    {
        $self->storage->debugfh->print("$_\n") if $self->storage->debug;
#        print "Running \n>>$_<<\n";
        $self->storage->dbh->do($_) or warn "SQL was:\n $_";
    }

    return 1;
}

=head1 NAME

DBIx::Class::Versioning - DBIx::Class::Schema plugin for Schema upgrades

=head1 SYNOPSIS

  package Library::Schema;
  use base qw/DBIx::Class::Schema/;   
  # load Library::Schema::CD, Library::Schema::Book, Library::Schema::DVD
  __PACKAGE__->load_classes(qw/CD Book DVD/);

  __PACKAGE__->load_components(qw/Versioning/);
  __PACKAGE__->upgrade_directory('/path/to/upgrades/');

  sub backup
  {
    my ($self) = @_;
    # my special backup process
  }

  sub upgrade
  {
    my ($self) = @_;

    ## overridable sub, per default just runs all the commands.

    $self->run_upgrade(qr/create/i);
    $self->run_upgrade(qr/alter table .*? add/i);
    $self->run_upgrade(qr/alter table .*? (?!drop)/i);
    $self->run_upgrade(qr/alter table .*? drop/i);
    $self->run_upgrade(qr/drop/i);
    $self->run_upgrade(qr//i);   
  }

=head1 DESCRIPTION

This module is a component designed to extend L<DBIx::Class::Schema>
classes, to enable them to upgrade to newer schema layouts. To use this
module, you need to have called C<create_ddl_dir> on your Schema to
create your upgrade files to include with your delivery.

A table called I<SchemaVersions> is created and maintained by the
module. This contains two fields, 'Version' and 'Installed', which
contain each VERSION of your Schema, and the date+time it was installed.

If you would like to influence which levels of version change need
upgrades in your Schema, you can override the method C<ddl_filename>
in L<DBIx::Class::Schema>. Return a false value if there is no upgrade
path between the two versions supplied. By default, every change in
your VERSION is regarded as needing an upgrade.


=head1 METHODS

=head2 backup

This is an overwritable method which is called just before the upgrade, to
allow you to make a backup of the database. Per default this method attempts
to call C<< $self->storage->backup >>, to run the standard backup on each
database type. 

=head2 upgrade

This is an overwritable method used to run your upgrade. The freeform method
allows you to run your upgrade any way you please, you can call C<run_upgrade>
any number of times to run the actual SQL commands, and in between you can
sandwich your data upgrading. For example, first run all the B<CREATE>
commands, then migrate your data from old to new tables/formats, then 
issue the DROP commands when you are finished.

=head2 run_upgrade

 $self->run_upgrade(qr/create/i);

Runs a set of SQL statements matching a passed in regular expression. The
idea is that this method can be called any number of times from your
C<upgrade> method, running whichever commands you specify via the
regex in the parameter.

=head1 AUTHOR

Jess Robinson <castaway@desert-island.demon.co.uk>
