package Subfeedr::Worker;
use Moose;
extends 'Tatsumaki::Service';

use Subfeedr::DataStore;
use Tatsumaki::HTTPClient;
use Tatsumaki::MessageQueue;
use Time::HiRes;
use Try::Tiny;
use AnyEvent;
use JSON;
use XML::Feed;
use XML::Smart;
use Digest::SHA;

our $FeedInterval = $ENV{SUBFEEDR_INTERVAL} || 60 * 15;

my %subscriber_timer;

sub start {
    my $self = shift;

    my $t; $t = AE::timer 0, 15, sub {
        scalar $t;
        my $ds = Subfeedr::DataStore->new('known_feed');
        my $cv = $ds->sort('set', by => 'next_fetch.*', get => 'feed.*', limit => "0 20");
        $cv->cb(sub {
            # Use cv to catch errors ERR: no such key exist
            my $cv = shift;
            try {
                my $feeds = shift;
                for my $feed (map JSON::decode_json($_), @$feeds) {
                    next if $feed->{next_fetch} && $feed->{next_fetch} > time;
                    $self->work_url($feed->{url});
                }
            }
        });
    };

    my $mq = Tatsumaki::MessageQueue->instance('feed_fetch');
    $mq->poll("worker", sub {
        my $url = shift;
        $self->work_url($url);
    });
}

sub work_url {
    my($self, $url) = @_;
    warn "Polling $url\n";

    Tatsumaki::HTTPClient->new->get($url, sub {
        my $res = shift;
        my $sha1 = Digest::SHA::sha1_hex($url);

        try {
            my $feed = XML::Feed->parse(\$res->content) or die "Parsing feed ($url) failed";

            my @new;
            my $db_entries = [];
            my $cv = AE::cv;
            $cv->begin(sub { $self->notify($sha1, $url, $feed, $db_entries) if @$db_entries });

            for my $entry ($feed->entries) {
                next unless $entry->id;
                $cv->begin;

                my $entry_sha = Digest::SHA::sha1_hex($entry->id);
                my $key = join ".", $sha1, $entry_sha;

                my $json = JSON::encode_json({
                    sha1 => $entry_sha,
                    feed => $sha1,
                    id   => $entry->id,
                    content => $entry->content->body
                });

                my $sha_json = Digest::SHA::sha1_hex($json);
                Subfeedr::DataStore->new('entry')->get($key, sub {
                    my $v = $_[0] ? Digest::SHA::sha1_hex(shift) : undef;
                    if (!$v or $v ne $sha_json) {
                        push @new, $entry;
                        push @$db_entries, {
                            key => $key,
                            json => $json,
                            entry => $entry,
                        };
                        #Subfeedr::DataStore->new('entry')->set($key, $json);
                    }
                    $cv->end;
                });
            }
            $cv->end;
        } catch {
            warn "Fetcher ERROR: $_";
        };

        # TODO smart polling
        my $time = Time::HiRes::gettimeofday + $FeedInterval;
        $time += 60 * 60 if $res->is_error;
        warn "Scheduling next poll for $url on $time\n";

        Subfeedr::DataStore->new('next_fetch')->set($sha1, $time);
        Subfeedr::DataStore->new('feed')->set($sha1, JSON::encode_json({
            sha1 => $sha1,
            url  => "$url",
            next_fetch => $time,
        }));
    });
}

sub notify {
    my($self, $sha1, $url, $feed, $db_entries) = @_;

    # assume that entries will only contain new updates from the feed
    my $how_many = @$db_entries;
    warn "Found $how_many entries for $url\n";

    my @new_entries = map ($_->{entry}, @$db_entries);
    my $payload = $self->post_payload($feed, \@new_entries);
    my $mime_type = $feed->format =~ /RSS/ ? 'application/rss+xml' : 'application/atom+xml';

    #Store entries in redis after subscribers have been processed.
    my $subscriber_cv = AE::cv;
    $subscriber_cv->begin ( sub {
        for my $db_entry (@$db_entries) { 
            Subfeedr::DataStore
                ->new('entry')
                ->set($db_entry->{key}, $db_entry->{json}); 
        }
    });

    Subfeedr::DataStore->new('subscription')->sort($sha1, get => 'subscriber.*', sub {
        my $subs = shift;

        for my $subscriber (map JSON::decode_json($_), @$subs) {
            $subscriber_cv->begin;
            my $subname = $subscriber->{sha1};
            my $subscriber_db_entries = [];
            my $total_payload;
            my $list_length;

            #remove previous subscriber timer
            undef $subscriber_timer{$subname};

            #Try to post payload to subscriber
            my $payload_cv = AE::cv;
            $payload_cv->begin( sub {
                my $hmac = Digest::SHA::hmac_sha1_hex(
                $total_payload, $subscriber->{secret});
                my $req = HTTP::Request->new(POST => $subscriber->{callback});
                $req->content_type($mime_type);
                $req->header('X-Hub-Signature' => "sha1=$hmac");
                $req->content_length(length $total_payload);
                $req->content($total_payload);

                my $http_client = Tatsumaki::HTTPClient->new;

                #set a timer to post to the subscriber callback.  Try every 30
                #seconds until we can successfully post to it.
                $subscriber_timer{$subname} = AE::timer 0, 30, sub {
                    $http_client->request($req, sub {
                        my $res = shift;
                        if ($res->is_error) {
                            warn $res->status_line;
                            warn $res->content;
                        } else {
                            #cancel the timer and empty the entry list for
                            #this subscriber
                            my $list_cv = AE::cv;
                            $list_cv->begin(sub {
                                undef $subscriber_timer{$subname};
                            });
                            for (0 .. $list_length) {
                                $list_cv->begin;
                                Subfeedr::DataStore
                                    ->new('subscriber_payload')
                                    ->lpop($subname, sub { 
                                    $list_cv->end;
                                });
                            }
                            $list_cv->end;
                        }
                    });
                };
            });

            my $entry_cv = AE::cv;

            #Generate payload to post and add to DB
            $entry_cv->begin( sub {
                my @subscriber_entries = map ($_->{entry}, @$subscriber_db_entries);
                my $subscriber_payload = $self->post_payload($feed, \@subscriber_entries);
                Subfeedr::DataStore
                    ->new('subscriber_payload')
                    ->rpush($subname, $subscriber_payload, sub {
                    for my $subscriber_db_entry (@$subscriber_db_entries) {
                        Subfeedr::DataStore->new("entry_$subname")
                            ->set($subscriber_db_entry->{key}, $subscriber_db_entry->{json});
                    }
                    Subfeedr::DataStore
                        ->new('subscriber_payload')
                        ->llen($subname, sub {
                        $list_length = shift;
                        --$list_length;
                        Subfeedr::DataStore
                            ->new('subscriber_payload')
                            ->lrange($subname, 0, $list_length, sub {
                                my $payloads = shift;
                                my $payload_object = XML::Smart->new($payloads->[0]);
                                #Assuming ATOM format
                                #TODO: handle RSS format
                                my $entries = $payload_object->{feed}{entry};
                                for my $idx (1 .. $list_length) {
                                    my $next_payload_object 
                                        = XML::Smart->new($payloads->[$idx]);
                                    push @$entries, @{$next_payload_object->{feed}{entry}};
                                }
                                $total_payload = $payload_object->data;
                                $payload_cv->end;
                        }); 
                    });
                });
            });

            #Calculate subscriber's new entries and add to db for seen entries
            for my $db_entry (@$db_entries) { 
                $entry_cv->begin;
                my $sha_json = Digest::SHA::sha1_hex($db_entry->{json});
                Subfeedr::DataStore->new("entry_$subname")->get($db_entry->{key}, sub {
                    my $v = $_[0] ? Digest::SHA::sha1_hex(shift) : undef;
                    if (!$v or $v ne $sha_json) {
                        push @$subscriber_db_entries, {
                            key => $db_entry->{key},
                            json => $db_entry->{json},
                            entry => $db_entry->{entry},
                        };
                        #Subfeedr::DataStore->new("entry_$subname")
                        #    ->set($db_entry->{key}, $db_entry->{json});
                    }
                    $entry_cv->end;
                });
            }
            $entry_cv->end;
            $subscriber_cv->end;
        }
        $subscriber_cv->end;
    });
}

sub post_payload {
    my($self, $feed, $entries) = @_;

    local $XML::Atom::ForceUnicode = 1;

    # TODO create XML::Feed::Diff or something to do this
    my $format = (split / /, $feed->format)[0];

    my $new = XML::Feed->new($format);
    for my $field (qw( title link description language author copyright modified generator )) {
        my $val = $feed->$field();
        next unless defined $val;
        $new->$field($val);
    }

    for my $entry (@$entries) {
        $new->add_entry($entry->convert($format));
    }

    my $payload = $new->as_xml;
    utf8::encode($payload) if utf8::is_utf8($payload); # Ugh

    return $payload;
}

1;
