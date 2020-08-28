use tiberius::{AuthMethod, Client, Config};
use tiberius::SqlBrowser;
use tokio::net::TcpStream;
use tokio_util::compat::Tokio02AsyncWriteCompatExt;
// use futures::StreamExt;
use serde::{Deserialize, Serialize};
// use anyhow::Result;
use docopt::Docopt;

const USAGE: &'static str = "
Naval Fate.

Usage:
  resgate_mssql [--nats_uri=<kn>]

Options:
  -h --help     Show this screen.
  --version     Show version.
  --nats_uri=<kn>  Speed in knots [default: localhost:4222].
  --host=<kn>  SQL host [default: localhost].
  --database=<kn>  SQL database [default: nrm72kulga].
  --instance=<kn>  SQL database [default: DE2012X64].
  --drifting    Drifting mine.
";

#[derive(Debug, Deserialize)]
struct Args {
    flag_speed: isize,
    flag_drifting: bool,
    nats_uri: Vec<String>,
    arg_x: Option<i32>,
    arg_y: Option<i32>,
    cmd_ship: bool,
    cmd_mine: bool,
}

#[derive(Serialize, Deserialize)]
struct Model {
    message: String,
}

#[derive(Serialize, Deserialize)]
struct ResultR {
    model: Model,
}

#[derive(Serialize, Deserialize)]
struct CallResponse {
    result: String,
}

#[derive(Serialize, Deserialize)]
struct Response {
    result: ResultR,
}
#[derive(Serialize, Deserialize)]
struct ResultAccess {
    get: bool,
    call: String,
}
#[derive(Serialize, Deserialize)]
struct Access {
    result: ResultAccess,
}
#[derive(Deserialize)]
struct Data {
    params: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = nats::Options::new();
    let args = Docopt::new(USAGE)
        .and_then(|dopt| dopt.parse())
        .unwrap_or_else(|e| e.exit());
    println!("{:?}", args);

    // You can conveniently access values with `get_{bool,count,str,vec}`
    // functions. If the key doesn't exist (or if, e.g., you use `get_str` on
    // a switch), then a sensible default value is returned.
    println!("\nSome values:");
    println!("  NATS: {}", args.get_str("--nats_uri"));

    let nc = opts
        // .with_name("nats-box rust 1")
        .connect(&args.get_str("--nats_uri"))?;

    nc.subscribe("access.example.model")?
        .with_handler(move |msg| {
            println!("Received {}", &msg);
            let response = Access {
                result: ResultAccess {
                    get: true,
                    call: "*".to_owned(),
                },
            };

            let j = serde_json::to_vec(&response)?;
            // println!("Reply {}", j);
            msg.respond(j)?;
            Ok(())
        });

    let mut config = Config::new();

    println!("  host: {}", args.get_str("--host"));
    config.host(args.get_str("--host"));

    // The default port of SQL Browser
    config.port(1433);
    config.database(args.get_str("--database"));

    config.trust_cert();

    // // And from here on continue the connection process in a normal way.
    // let mut client = Client::connect(config, tcp).await?;

    // Using SQL Server authentication.
    config.authentication(AuthMethod::sql_server("bot", "bot"));

    // // Taking the address from the configuration, using async-std's
    // // TcpStream to connect to the server.
    // let tcp = TcpStream::connect(config.get_addr()).await?;

    // The name of the database server instance.
    config.port(1434);
    config.instance_name(args.get_str("--instance"));
    // This will create a new `TcpStream` from `async-std`, connected to the
    // right port of the named instance.
    let tcp = TcpStream::connect_named(&config).await?;
    // let mut client = Client::connect(config, tcp).await?;
    // let tcp = TcpStream::connect(config.get_addr()).await?;
    // We'll disable the Nagle algorithm. Buffering is handled
    // internally with a `Sink`.
    // tcp.set_nodelay(true)?;

    // Handling TLS, login and other details related to the SQL Server.
    let mut client = Client::connect(config, tcp.compat_write()).await?;

    let sub = nc.subscribe("call.example.model.sql")?;
    for msg in sub.messages() {
        println!("Received a {}", msg);
        let data: Data = serde_json::from_slice(&msg.data[..])?;

        let mut stream = client.simple_query(data.params).await?;

        // As long as the `next_resultset` returns true, the stream has
        // more results and can be polled. For each result set, the stream
        // returns rows until the end of that result. In a case where
        // `next_resultset` is true, polling again will return rows from
        // the next query.
        assert!(stream.next_resultset());

        // In this case, we know we have only one query, returning one row
        // and one column, so calling `into_row` will consume the stream
        // and return us the first row of the first result.
        let row = stream.into_row().await?;

        // assert_eq!(Some(-4i32), row.unwrap().get(0));
        let r = row.unwrap();
        let payload: &str = r.get(0).unwrap();
        println!("{}", payload);

        let response = CallResponse {
            result: payload.to_owned(),
        };

        // println!("response {}", &response);
        let j = serde_json::to_vec(&response)?;
        // println!("Reply to {}", msg.reply.to_owned().unwrap());
        // println!("Reply {}", j.to_string());
        match msg.respond(j) {
            Ok(v) => println!("sended: {:?}", v),
            Err(e) => println!("error: {:?}", e),
        }
    }

    Ok(())
}