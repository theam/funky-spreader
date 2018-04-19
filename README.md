# funky-spreader
_Spread MySQL events to Kafka, in a func-y way_


## Requirements

* MySQL-complaint database installation
  * You should have a database named `spreader` created
  * Binary logs should be enabled
* A basic, local, Kafka installation
* .NET Core

## How it works

Basically, `funky-spreader` will stream from the binary log in
the specified file, sending all the new events to Kafka.

By the time that this demo is written, a proper binary log parser
doesn't exist for .NET, so we opted by sending the raw messages.

## Usage

Generally, binary logs are saved in `/var/lib/mysql`. Either save
them in another directory where your user has access, or run this
demo with `sudo`.

1. Clone this repo
2. Run `dotnet run -- /path/to/the/latest/binlog/file`



