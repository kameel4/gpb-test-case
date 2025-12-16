use strict;
use warnings;

use IO::Socket::INET;
use URI::Escape qw(uri_unescape);
use Template;
use lib 'lib';
use db_interface;

db_interface::connect_db('eximlogs','localhost', 3306,"root","");

my $tt = Template->new({
    INCLUDE_PATH => 'templates',
    ENCODING => 'utf8'
    }) or die Template->error;

my $server = IO::Socket::INET->new(
    LocalAddr   => '127.0.0.1',
    LocalPort   => 3000,
    Proto       => 'tcp',
    Listen      => 10,
    ReuseAddr   => 1,
    ) or die "Can't listen: $!";

print "Open http://127.0.0.1:3000/\n";

while (my $client = $server->accept) {
    $client->autoflush(1);

    my $request_line = <$client>;
    if (!defined $request_line) {close $client; next; }
    chomp $request_line;
    $request_line =~ s/\r$//;
    print($request_line);

    my ($method, $target) = $request_line =~ m{^(\w+)\s+(\S+)} ? ($1, $2) : ('', '/');

    while (my $line = <$client>) {
        last if $line eq "\r\n" || $line eq "\n";
    }

    if ($method ne 'GET') {
        print $client "HTTP/1.1 405 Method Not Allowed\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\nOnly GET\n";
        close $client;
        next;
    }

    my ($path , $query) = split /\?/, $target, 2;
    $query //= '';

    my %p;
    for my $pair (split /&/, $query) {
        next if $pair eq '';
        my ($k, $v) = split /=/, $pair, 2;
        $k = uri_unescape($k // '');
        $v = uri_unescape($v // '');
        $v =~ tr/+/ /;
        $p{$k} = $v;
    }

    my $rcpt = $p{rcpt} // '';
    $rcpt =~ s/^\s+|\s+$//g;

    my ($messages, $error) = ([], undef);

    if ($rcpt ne ''){
        eval {
            $messages = db_interface::get_rows_by_address($rcpt);
            1;
        } or do {
            $error = $@;
            warn $error;
            $messages = [];
        };
    }

    my $html = '';
    print(@$messages);
    $tt->process('messages.tt2', {
        rcpt       => $rcpt,
        messages   => $messages,
        error      => $error,
    }, \$html) or do {
        $html = "Template error: " . $tt->error;
    };

    my $len = length($html);
    print $client
        "HTTP/1.1 200 OK\r\n" .
        "Content-Type: text/html; charset=UTF-8\r\n" .
        "Content-Length: $len\r\n" .
        "Connection: close\r\n" .
        "\r\n" .
        $html;

    close $client;
}