use tiberius::SqlBrowser;
use tiberius::{AuthMethod, Client, Config, FromSqlOwned};
use tokio::net::TcpStream;
// use async_std::net::TcpStream;
use tokio_util::compat::Tokio02AsyncWriteCompatExt;
// use futures::StreamExt;
use serde::{Deserialize, Serialize};
// use anyhow::Result;
use docopt::Docopt;

const USAGE: &'static str = "
nats-resgate-mssql-service.

Usage:
  nats-resgate-mssql-service [--nats_uri=<uri>] [--host=<kn>] [--database=<kn>]  [--instance=<kn>]

Options:
  --nats_uri=<kn>  Speed in knots [default: localhost:4222].
  --host=<h>  SQL host [default: localhost].
  --database=<kn>  SQL database [default: nrm72kulga].
  --instance=<kn>  SQL database.
";

// #[derive(Debug, Deserialize)]
// struct Args {
//     flag_speed: isize,
//     flag_drifting: bool,
//     nats_uri: Vec<String>,
//     arg_x: Option<i32>,
//     arg_y: Option<i32>,
//     cmd_ship: bool,
//     cmd_mine: bool,
// }

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

// #[async_std::main]
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
    config.database(args.get_str("--database"));

    config.trust_cert();

    // // And from here on continue the connection process in a normal way.
    // let mut client = Client::connect(config, tcp).await?;

    // Using SQL Server authentication.
    config.authentication(AuthMethod::sql_server("bot", "bot"));

    // // Taking the address from the configuration, using async-std's
    // // TcpStream to connect to the server.
    // let tcp = TcpStream::connect(config.get_addr()).await?;

    let instance: &str = args.get_str("--instance");
    let tcp: TcpStream;
    // let mut client: Client<TcpStream>;
    println!("  instance: {}", instance);
    if instance == "" {
        config.port(1433);
        tcp = TcpStream::connect(config.get_addr()).await?;
    //   mut client = Client::connect(config, tcp).await?;
    } else {
        // The name of the database server instance.
        config.port(1434);
        config.instance_name(instance);
        // This will create a new `TcpStream` from `async-std`, connected to the
        // right port of the named instance.
        tcp = TcpStream::connect_named(&config).await?;
    }
    // We'll disable the Nagle algorithm. Buffering is handled
    // internally with a `Sink`.
    tcp.set_nodelay(true)?;
    let mut client = Client::connect(config, tcp.compat_write()).await?;

    // Handling TLS, login and other details related to the SQL Server.

    let sub = nc.subscribe("call.example.model.*")?;
    for msg in sub.messages() {
        println!("Received a {}", msg);
        match nc.publish(
            msg.reply.clone().unwrap_or_default().as_str(),
            "timeout:\"30000\"",
        ) {
            Ok(v) => println!("sended: {:?}", v),
            Err(e) => println!("error: {:?}", e),
        }
        let data: Data = serde_json::from_slice(&msg.data[..])?;
        let split = msg.subject.split(".");
        let v: Vec<&str> = split.collect();
        let method = v.last().unwrap().as_ref();
        match method {
            "exec" => {
                let result = client.execute(data.params, &[]).await?;

                // As long as the `next_resultset` returns true, the stream has
                // more results and can be polled. For each result set, the stream
                // returns rows until the end of that result. In a case where
                // `next_resultset` is true, polling again will return rows from
                // the next query.
                // result.rows_affected()
                // assert!(stream.next_resultset());
                // In this case, we know we have only one query, returning one row
                // and one column, so calling `into_row` will consume the stream
                // and return us the first row of the first result.
                let payload = format!("{}", result.total());
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
            "simple_query" => println!("simple_query"),
            // Handle the rest of cases
            _ => {
                let mut stream = client.simple_query(data.params).await?;

                // As long as the `next_resultset` returns true, the stream has
                // more results and can be polled. For each result set, the stream
                // returns rows until the end of that result. In a case where
                // `next_resultset` is true, polling again will return rows from
                // the next query.
                // assert!(stream.next_resultset());
                if stream.next_resultset() {
                    // In this case, we know we have only one query, returning one row
                    // and one column, so calling `into_row` will consume the stream
                    // and return us the first row of the first result.
                    let row = stream.into_row().await?;
                    match row {
                        Some(r) => {
                            // let payload: &str = r.get(0).unwrap();
                            for val in r.into_iter() {
                                // assert_eq!(
                                //     Some(String::from("test")),
                                //     String::from_sql_owned(val)?
                                // )
                                // let pp = r.get(0);
                                // let ss = String::from_sql_owned(pp);
                                match String::from_sql_owned(val) {
                                    Ok(payload_option) => {
                                        // let payload: &str = payloadv.as_str();
                                        match payload_option {
                                            Some(payload) => {
                                                println!("{}", payload);

                                                let response = CallResponse { result: payload };

                                                // println!("response {}", &response);
                                                let j = serde_json::to_vec(&response)?;
                                                // println!("Reply to {}", msg.reply.to_owned().unwrap());
                                                // println!("Reply {}", j.to_string());
                                                match msg.respond(j) {
                                                    Ok(v) => println!("sended: {:?}", v),
                                                    Err(e) => println!("error: {:?}", e),
                                                }
                                            }
                                            None => {
                                                println!("none value");
                                                let response = CallResponse { result: String::from("none") };
                                                // "error": {
                                                //     "code": "system.invalidParams",
                                                //     "message": "Invalid parameters"
                                                // }
                                                // println!("response {}", &response);
                                                let j = serde_json::to_vec(&response)?;
                                                // println!("Reply to {}", msg.reply.to_owned().unwrap());
                                                // println!("Reply {}", j.to_string());
                                                match msg.respond(j) {
                                                    Ok(v) => println!("sended: {:?}", v),
                                                    Err(e) => println!("error: {:?}", e),
                                                }

                                            },
                                        }
                                    }
                                    Err(e) => println!("error: {:?}", e),
                                }
                            }
                        }
                        None => println!("none row"),
                    }
                }
            }
        }
    }

    Ok(())
}
