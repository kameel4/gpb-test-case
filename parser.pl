use warnings;
use strict;
use 5.010;
use lib 'lib';
use db_interface;

sub calc_status {
    my ($data) = @_;
    my $flag = $data->{flag} // '';
    my $s    = $data->{str}  // '';

    return 'failed' if $flag eq '**';
    return 'delivered' if $s =~ /\bCompleted\b/i;
    return undef;
}


my $logfile_name = "maillog";
open my $logfile_handle, "<", $logfile_name or die "Can't open maillog, $!";

my $re = qr/
    (?:
        (?<date>\d{4}-\d{2}-\d{2})
      | (?<time>\d{2}:\d{2}:\d{2})
      | (?<inner_id>.{6}-.{6}-.{2})
      | (?<flag>[<>=\-\*]{2})
      | (?<address>[\w\.]+@[\w\.]+|:blackhole:)
      | H=(?<domain>[\S]+)\s\[(?<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\]
      | R=(?<router_name>[\S]+)
      | T=(?<topic>[\S]+)
      | C="(?<confirmation>[^"]*)"
      | S=(?<size>[\S]+)
      | P=(?<protocol>[\S]+)
      | X=(?<cipher>[\S]+)
      | id=(?<id>[\S]+)
      | defer (?<defer>[\S]+)
    ) 
/x;

my $n = 0;
db_interface::connect_db("eximlogs", "localhost", 3306, "root", "");
for (my $i = 0; $i < 10000; $i++){
    my $line = <$logfile_handle>;
    chomp $line;

    my $raw = $line;
    my %data;
    while ($line =~ /$re/g) {
        $data{date} = $+{date} if defined $+{date};
        $data{time} = $+{time} if defined $+{time};
        $data{inner_id} = $+{inner_id} if defined $+{inner_id};
        $data{flag} = $+{flag} if defined $+{flag};
        $data{address} = $+{address} if defined $+{address};
        $data{domain} = $+{domain} if defined $+{domain};
        $data{ip} = $+{ip} if defined $+{ip};
        $data{router_name} = $+{router_name} if defined $+{router_name};
        $data{topic} = $+{topic} if defined $+{topic};
        $data{confirmation} = $+{confirmation} if defined $+{confirmation};
        $data{protocol} = $+{protocol} if defined $+{protocol};
        $data{cipher} = $+{cipher} if defined $+{cipher};
        $data{id} = $+{id} if defined $+{id};
        $data{defer} = $+{defer} if defined $+{defer};
        $data{size} = $+{size} if defined $+{size};
    }
    
    my $rest = $raw;
    $rest =~ s/$re/ /g;
    $rest =~ s/\s+/ /g;
    $rest =~ s/^\s+|\s+$//g;

    $data{str} = $rest;

    # foreach my $key (keys %data) {
    #     print "$key => $data{$key}\n"
    # }
    # print "\n\n";

    # 1) маппинг в колонки log
    my $int_id  = $data{inner_id};
    my $created = ($data{date} // '') . ' ' . ($data{time} // '');

    $created =~ s/^\s+|\s+$//g;

    my $flag      = $data{flag};
    my $address   = $data{address};

    my @str_parts;
    if (defined $address && $address eq ':blackhole:') {
    push @str_parts, ':blackhole:';
    $address = undef;
    }

    my $domain = undef;
    if (defined $address && $address =~ /\@(.+)$/) {
    $domain = $1;
    }

    my $host = undef;
    if (defined $data{domain} || defined $data{ip}) {
    $host = join(' ', grep { defined && length } (
        $data{domain},
        (defined $data{ip} ? "[$data{ip}]" : undef),
    ));
    }

    push @str_parts, $data{str}                 if defined $data{str} && length $data{str};
    push @str_parts, "R=$data{router_name}"     if defined $data{router_name};
    push @str_parts, "T=$data{topic}"           if defined $data{topic};
    push @str_parts, "P=$data{protocol}"        if defined $data{protocol};
    push @str_parts, "X=$data{cipher}"          if defined $data{cipher};
    push @str_parts, "id=$data{id}"             if defined $data{id};
    push @str_parts, "S=$data{size}"            if defined $data{size};
    push @str_parts, "C=$data{confirmation}"    if defined $data{confirmation};
    push @str_parts, "defer $data{defer}"       if defined $data{defer};

    my $str = join(' ', grep { defined && length } @str_parts);
    my $status = calc_status(\%data);

    if (!defined $int_id || $int_id eq '') {
        warn "NO inner_id at input line $.: $raw\n";
        next;
    }

    db_interface::insert_log($int_id, $created, $data{flag},
        $data{address}, $domain, $host, $str
    );

    my $env_from = (defined $data{flag} && $data{flag} eq '<=') ? $data{address} : undef;
    db_interface::upsert_message($int_id, $created, $data{id}, $env_from, $data{str}, $status);

    $n++;
    db_interface::commit() if $n % 1000 == 0;

}

db_interface::commit();
db_interface::disconnect_db();
