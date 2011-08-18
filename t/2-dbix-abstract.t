#!/usr/bin/perl

# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl 1.t'

#########################

use Test::More tests => 28;

#########################

unless (defined(do 't/dbia.config')) {
    die $@ if $@;
    unless (defined(do 'dbia.config')) {
        die $@ if $@;
        die "Could not reade dbia.config: $!\n";
    }
}
my %opt = load_all();

$conn = {
    user => $opt{'user'} || undef,
    password => $opt{'password'} || undef,
    };
if ($opt{'dsn'}) {
    $$conn{'dsn'} = $opt{'dsn'};
} else {
    $$conn{'driver'}   = $opt{'driver'};
    $$conn{'dbname'}   = $opt{'db'};
    $$conn{'host'}     = $opt{'host'};
    $$conn{'port'}     = $opt{'port'};
}

eval {
    require DBIx::Abstract;
}; is($@, '', 'loading module');

eval {
    import DBIx::Abstract;
}; is($@, '', 'running import');

my $dsn;

SKIP: {
    skip("Can't do database tests if you don't specify a driver",26)
        unless $$conn{'driver'};

    eval {
        $dbh = DBIx::Abstract->connect($conn);
        $dsn = $dbh->{'connect'}{'datasource'};
        if ($dbh->{'dbh'}->{'Driver'}->{'Name'} eq 'mysql' or
            $dbh->{'dbh'}->{'Driver'}->{'Name'} eq 'mysqlPP') {
            $$conn{'dialect'} = 'MySQL';
        }
    }; is(ref($dbh), 'DBIx::Abstract', 'connect dbname');


    eval {
        $dbh->disconnect if $dbh;
        my $dbi = DBI->connect($dsn,$$conn{'user'},$conn{'password'});
        $dbh = DBIx::Abstract->connect($dbi);
    }; is(ref($dbh), 'DBIx::Abstract', 'connect with dbi object');

    unlink('test.log');

    eval {
        $dbh->disconnect if $dbh;
        $dbh = DBIx::Abstract->connect($conn,{
            loglevel=>5,
            logfile=>'test.log',
            });
    }; is($@ || ref($dbh), 'DBIx::Abstract', 'connect db');

    eval {
        $dbh->disconnect if $dbh;
        $dbh->reconnect if $dbh;
    };

    ok( ! $@, "Reconnect: No error");
    ok( $dbh, "Reconnect: Database handle exists");
    ok( $dbh->connected(), "Reconnect: We are indeed connected." );

    eval {
        my $dbih = $dbh->{'dbh'};
        my $dbh2 = DBIx::Abstract->connect($dbih);
        $dbh2->DESTROY();
    };
    ok( ! $@, "connect w/dbhandle and destroy: No error");
    ok( $dbh, "connect w/dbhandle and destroy: Database handle exists");
    ok( $dbh->connected(), "connect w/dbhandle and destroy: We are indeed connected." );

    is( @{$dbh->{'CLONES'}}, 0, "no clones yet");
    eval {
       eval {
           my $dbh2 = $dbh->clone();
           is( @{$dbh->{'CLONES'}}, 1, "one clone now");
       };
       is( @{$dbh->{'CLONES'}}, 0, "clone cleaned up");
    };
    ok( ! $@, "clone without error");

    eval {
        $dbh->query('create table foo (id int null,name char(30) not null,value char(30) null)');
        $dbh->query('create table bar (id int null,foo_id int null,name char(30) not null)');
    }; is($@,'','create');

    eval {
        $dbh->insert('foo',{id=>1,name=>'test',value=>'this'});
        $dbh->insert('foo',{id=>2,name=>'bar',value=>'baz'});
        $dbh->insert('foo',{id=>3,name=>'this',value=>'test'});
        $dbh->insert('foo',{id=>4,name=>'baz',value=>'bar'});
        $dbh->insert('bar',{id=>1,foo_id=>4,name=>'heh'});
        $dbh->insert('bar',{id=>2,foo_id=>3,name=>'heh'});
        $dbh->insert('bar',{id=>3,foo_id=>2,name=>'heh'});
        $dbh->insert('bar',{id=>4,foo_id=>1,name=>'baz'});
        $count1=4;
    }; is($@,'','insert');

    eval {
        $dbh->update('foo',{name=>'blat', value=>'bonk'},{id=>2});
    }; is($@,'','update');

    eval {
        $dbh->select('*','foo',{id=>['<',10]});
        $dbh->rows;
        while(@foo = $dbh->fetchrow_array) { $count2 ++ }
    }; is((!$@ and $count1==$count2)?1:0,1,"select ($count1==$count2)");


    eval {
        my @foo;
        @foo = ({id=>['<',10]},'and',\@foo);
        $dbh->select('*','foo',\@foo);
        if ($dbh->rows) {
            while(@foo = $dbh->fetchrow_array) { $count2 ++ }
        }
    }; is($@?1:0,1,'circular where');

    eval {
        $dbh->select('*','foo',[{id=>['<',10]},'and',[{name=>'blat'},'or',{value=>'bonk'}]]);
        $dbh->rows;
        while(@foo = $dbh->fetchrow_array) { $count2 ++ }
    }; is($@,'','select with complex where');

    eval {
        $dbh->select({
            fields=>'count(foo.id)',
            tables=>'foo,bar',       
            'join'=>[
                     'foo.id = bar.foo_id', 
                     ],
            where=>{'foo.id'=>['<',10]},
            group=>'bar.name',
            });
        if ($dbh->rows) {
            while(@foo = $dbh->fetchrow_array) { $count2 ++ }
        }
    }; is($@,'','select with join');

    eval {
        $dbh->delete('foo',{id=>['like','%']});
    }; is((!$@ and $count1==$dbh->rows)?1:0,1,'delete');

    eval {
        $dbh->query('drop table foo');
        $dbh->query('drop table bar');
    }; ok( !$@,'drop');

    ok( $dbh->connected, "verified connection" );

    eval { 
        $dbh->disconnect;
    }; ok( ! $@,'disconnect');

    ok( ! $dbh->connected, "verified disconnection" );

    if (open(LOG,'test.log')) {
        my @log = <LOG>;
        close(LOG);
        my @data;
        my $ignore = 0;
        while (<DATA>) {
            if (/^[^\t]+\t0\t([^\t]+)\tSTART\n$/ and $1 ne $$conn{'dialect'}) {
                $ignore = $1;
            } elsif ($ignore and /^[^\t]+\t0\t$ignore\tEND\n$/) {
                $ignore = 0;
            } elsif (!$ignore) {
                push(@data,$_);
            }
        }
        foreach (\@log,\@data) {
            map({s/^[^\t]+/DATE/g} @$_);
            map({s/^(DATE\t5\t(?:Rec|C)onnect\t).*$/$1CONNECT ARGS/} @$_);
            map({s/^(DATE\t5\tconnected\t)\n$/${1}0\n/} @$_);
        }
        if (is_deeply( \@log, \@data, "SQL log matches expectations" )) {
            unlink('test.log');
        }
    }
}

__DATA__
Mon Feb 17 13:36:46 2003	5	Option change	logfile		test.log
Mon Feb 17 13:36:46 2003	5	Connect	driver=>mysqlPP	host=>	port=>
Mon Feb 17 13:36:46 2003	5	connected	0
Mon Feb 17 13:36:46 2003	5	reconnect	success
Mon Feb 17 13:36:46 2003	5	Reconnect	test
Mon Feb 17 13:36:46 2003	5	connected	1
Mon Feb 17 13:36:46 2003	5	connected	1
Mon Feb 17 13:36:46 2003	5	Cloned
Mon Feb 17 13:36:46 2003	3	create table foo (id int null,name char(30) not null,value char(30) null)
Mon Feb 17 13:36:46 2003	3	create table bar (id int null,foo_id int null,name char(30) not null)
Mon Feb 17 13:36:46 2003	1	INSERT INTO foo ( value, name, id) VALUES ('this', 'test', '1')
Mon Feb 17 13:36:46 2003	1	INSERT INTO foo ( value, name, id) VALUES ('baz', 'bar', '2')
Mon Feb 17 13:36:46 2003	1	INSERT INTO foo ( value, name, id) VALUES ('test', 'this', '3')
Mon Feb 17 13:36:46 2003	1	INSERT INTO foo ( value, name, id) VALUES ('bar', 'baz', '4')
Mon Feb 17 13:36:46 2003	1	INSERT INTO bar ( name, foo_id, id) VALUES ('heh', '4', '1')
Mon Feb 17 13:36:46 2003	1	INSERT INTO bar ( name, foo_id, id) VALUES ('heh', '3', '2')
Mon Feb 17 13:36:46 2003	1	INSERT INTO bar ( name, foo_id, id) VALUES ('heh', '2', '3')
Mon Feb 17 13:36:46 2003	1	INSERT INTO bar ( name, foo_id, id) VALUES ('baz', '1', '4')
Mon Feb 17 13:36:46 2003	1	UPDATE foo SET value='bonk', name='blat' WHERE id = '2'
Mon Feb 17 13:36:46 2003	2	SELECT * FROM foo WHERE id < '10'
Mon Feb 17 13:36:46 2003	5	rows
Mon Feb 17 13:36:46 2003	4	fetchrow_array
Mon Feb 17 13:36:46 2003	4	fetchrow_array
Mon Feb 17 13:36:46 2003	4	fetchrow_array
Mon Feb 17 13:36:46 2003	4	fetchrow_array
Mon Feb 17 13:36:46 2003	4	fetchrow_array
Mon Feb 17 13:36:46 2003	0	Where parser iterated too deep (limit of 20)
Mon Feb 17 13:36:46 2003	2	SELECT * FROM foo WHERE (id < '10') and ((name = 'blat') or (value = 'bonk'))
Mon Feb 17 13:36:46 2003	5	rows
Mon Feb 17 13:36:46 2003	4	fetchrow_array
Mon Feb 17 13:36:46 2003	4	fetchrow_array
Mon Feb 17 13:36:46 2003	2	SELECT count(foo.id) FROM foo,bar WHERE (foo.id < '10') and ( foo.id = bar.foo_id ) GROUP BY bar.name
Mon Feb 17 13:36:46 2003	5	rows
Mon Feb 17 13:36:46 2003	4	fetchrow_array
Mon Feb 17 13:36:46 2003	4	fetchrow_array
Mon Feb 17 13:36:46 2003	4	fetchrow_array
Mon Feb 17 13:36:46 2003	1	DELETE FROM foo WHERE id like '%'
Mon Feb 17 13:36:46 2003	5	rows
Mon Feb 17 13:36:46 2003	3	drop table foo
Mon Feb 17 13:36:46 2003	3	drop table bar
Mon Feb 17 13:36:46 2003	5	connected	1
Mon Feb 17 13:36:46 2003	5	connected	0
