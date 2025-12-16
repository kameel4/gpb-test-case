package db_interface;

use strict;
use warnings;
use DBI;


our ($DBH, $STH_LOG, $STH_UPSERT_MSG);

sub connect_db {
    my ($dbname, $host, $port, $user, $pass) = @_;

    my $dsn = "DBI:mysql:database=$dbname;host=$host;port=$port";

    $DBH = DBI->connect($dsn, $user, $pass, {
        RaiseError => 1,
        PrintError => 0,
        AutoCommit => 0
    }) or die "Cannot connect: $DBI::errstr";

    $STH_LOG = $DBH->prepare(q{
        INSERT INTO log (int_id, created, flag, address, domain, host, str)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    });

    $STH_UPSERT_MSG = $DBH->prepare(q{
        INSERT INTO message (id, int_id, created, envelope_from, str, status)
        VALUES (?, ?, ?, ?, ?, COALESCE(?, 'queued'))
        ON DUPLICATE KEY UPDATE
            id = CASE
                    WHEN id LIKE 'tmp:%' AND VALUES(id) NOT LIKE 'tmp:%' THEN VALUES(id)
                    ELSE id
                 END,
            created = CASE
                        WHEN created IS NULL THEN VALUES(created)
                        ELSE created
                      END,
            envelope_from = CASE
                              WHEN envelope_from IS NULL OR envelope_from = '' THEN VALUES(envelope_from)
                              ELSE envelope_from
                            END,
            status = CASE
                       WHEN VALUES(status) IS NULL THEN status
                       ELSE VALUES(status)
                     END,
            str = CASE
                    WHEN VALUES(str) IS NULL OR VALUES(str) = '' THEN str
                    WHEN str IS NULL OR str = '' THEN VALUES(str)
                    ELSE str
                  END
    });

    return $DBH;
}

sub disconnect_db {
    return unless $DBH;
    eval { $DBH->commit };
    $STH_LOG = undef;
    $STH_UPSERT_MSG = undef;
    $DBH->disconnect;
    $DBH = undef;
}

sub commit  { $DBH->commit  if $DBH }
sub rollback{ $DBH->rollback if $DBH }

sub insert_log {
    my ($int_id, $created, $flag, $address, $domain, $host, $str) = @_;
    $STH_LOG->execute($int_id, $created, $flag, $address, $domain, $host, ($str // ''));
}

sub upsert_message {
    my ($int_id, $created, $msg_id, $envelope_from, $str, $status) = @_;

    my $id = (defined $msg_id && $msg_id ne '') ? $msg_id : "tmp:$int_id";
    $str = '' unless defined $str;

    $STH_UPSERT_MSG->execute($id, $int_id, $created, $envelope_from, $str, $status);
}

sub get_rows_by_address {
    my ($rcpt) = @_;

    my @rows;
    my %seen_int_id;

    my $sth_log = $DBH->prepare(q{
        SELECT int_id, created, str, flag, NULL AS status, host
        FROM log
        WHERE address = ?
        OR str LIKE CONCAT('%', ?, '%')
        ORDER BY created DESC
        LIMIT 200
    });

    $sth_log->execute($rcpt, $rcpt);

    while (my $r = $sth_log->fetchrow_hashref) {
        push @rows, {
            type    => 'log',
            int_id  => $r->{int_id},
            created => $r->{created},
            flag    => $r->{flag},
            str     => "H=$r->{host} $r->{str}",
            status  => $r->{status}
        };
        $seen_int_id{ $r->{int_id} } = 1 if defined $r->{int_id};
    }

    $sth_log->finish;

    return \@rows unless %seen_int_id;

    my @int_ids = keys %seen_int_id;
    my $ph = join ',', ('?') x @int_ids;

    my $sth_msg = $DBH->prepare(qq{
        SELECT int_id, created, str, NULL AS flag, status
        FROM message
        WHERE int_id IN ($ph)
    });

    $sth_msg->execute(@int_ids);

    while (my $r = $sth_msg->fetchrow_hashref) {
        push @rows, {
            type    => 'message',
            int_id  => $r->{int_id},
            created => $r->{created},
            flag    => $r->{flag},
            str     => $r->{str},
            status  => $r->{status}
        };
    }

    $sth_msg->finish;

    @rows = sort {
        ($a->{int_id}  cmp $b->{int_id})
        ||
        ($a->{created} cmp $b->{created})
    } @rows;

    @rows = @rows[0..100];

    return \@rows;
}

1;
