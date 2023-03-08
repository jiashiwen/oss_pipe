use std::borrow::Borrow;
use std::process::Command;
use std::str::FromStr;
use std::{env, fs, thread};

use clap::{Arg, ArgMatches};
use clap::{ArgAction, Command as Clap_Command};
use fork::{daemon, Fork};
use lazy_static::lazy_static;
use sysinfo::{Pid, ProcessExt, ProcessRefreshKind, RefreshKind, System, SystemExt};
use tokio::runtime;

use crate::checkers::check_local_desc_path;
use crate::cmd::osstaskcmd::new_osstask_cmd;
use crate::cmd::{new_config_cmd, new_osscfg_cmd, new_start_cmd, new_stop_cmd};
use crate::commons::yamlutile::struct_to_yml_file;
use crate::commons::{read_lines, read_yaml_file, SubCmd};
use crate::commons::{struct_to_yaml_string, CommandCompleter};
use crate::configure::{generate_default_config, set_config_file_path};
use crate::configure::{get_config, get_config_file_path, get_current_config_yml, set_config};
use crate::interact;
use crate::osstask::{task_id_generator, TaskDownload, TaskTransfer};
use crate::s3::oss::OSSDescription;
use crate::s3::oss::OssProvider;

pub const APP_NAME: &'static str = "oss_pipe";
lazy_static! {
    static ref CLIAPP: Clap_Command = Clap_Command::new(APP_NAME)
        .version("1.0")
        .author("Shiwen Jia. <jiashiwen@gmail.com>")
        .about("RustBoot")
        .arg_required_else_help(true)
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file")
        )
        .arg(
            Arg::new("interact")
                .short('i')
                .long("interact")
                .action(ArgAction::SetTrue)
                .help("run as interact mod")
        )
        .subcommand(
            new_start_cmd().arg(
                Arg::new("daemon")
                    .short('d')
                    .long("daemon")
                    .help("run as daemon")
            )
        )
        .subcommand(new_osstask_cmd())
        .subcommand(new_stop_cmd())
        .subcommand(new_config_cmd())
        .subcommand(new_osscfg_cmd());
    static ref SUBCMDS: Vec<SubCmd> = subcommands();
}

pub fn run_app() {
    set_config("");
    let matches = CLIAPP.clone().get_matches();
    cmd_match(&matches);
}

pub fn run_from(args: Vec<String>) {
    match Clap_Command::try_get_matches_from(CLIAPP.to_owned(), args.clone()) {
        Ok(matches) => {
            cmd_match(&matches);
        }
        Err(err) => {
            err.print().expect("Error writing Error");
        }
    };
}

// 获取全部子命令，用于构建commandcompleter
pub fn all_subcommand(app: &Clap_Command, beginlevel: usize, input: &mut Vec<SubCmd>) {
    let nextlevel = beginlevel + 1;
    let mut subcmds = vec![];
    for iterm in app.get_subcommands() {
        subcmds.push(iterm.get_name().to_string());
        if iterm.has_subcommands() {
            all_subcommand(iterm, nextlevel, input);
        } else {
            if beginlevel == 0 {
                all_subcommand(iterm, nextlevel, input);
            }
        }
    }
    let subcommand = SubCmd {
        level: beginlevel,
        command_name: app.get_name().to_string(),
        subcommands: subcmds,
    };
    input.push(subcommand);
}

pub fn get_command_completer() -> CommandCompleter {
    CommandCompleter::new(SUBCMDS.to_vec())
}

fn subcommands() -> Vec<SubCmd> {
    let mut subcmds = vec![];
    all_subcommand(CLIAPP.clone().borrow(), 0, &mut subcmds);
    subcmds
}

fn cmd_match(matches: &ArgMatches) {
    if let Some(c) = matches.get_one::<String>("config") {
        set_config_file_path(c.to_string());
        set_config(&get_config_file_path());
    } else {
        set_config("");
    }

    if matches.get_flag("interact") {
        interact::run();
        return;
    }

    if let Some(ref matches) = matches.subcommand_matches("start") {
        if matches.get_flag("daemon") {
            let args: Vec<String> = env::args().collect();
            if let Ok(Fork::Child) = daemon(true, true) {
                // Start child thread
                let mut cmd = Command::new(&args[0]);
                for idx in 1..args.len() {
                    let arg = args.get(idx).expect("get cmd arg error!");
                    // remove start as daemon variable
                    // 去除后台启动参数,避免重复启动
                    if arg.eq("-d") || arg.eq("-daemon") {
                        continue;
                    }
                    cmd.arg(arg);
                }

                let child = cmd.spawn().expect("Child process failed to start.");
                fs::write("pid", child.id().to_string()).expect("Write pid file error!");
            }
            println!("{}", "daemon mod");
            std::process::exit(0);
        }

        println!("current pid is:{}", std::process::id());

        //校验本地任务目录是否存在，没有就创建
        check_local_desc_path().unwrap();

        // check config
        let cfg = get_config().unwrap();
        let task_ping_handle = thread::spawn(move || {
            let c = cfg.clone();

            let rt = runtime::Builder::new_multi_thread()
                .worker_threads(c.threads)
                .enable_io()
                .enable_time()
                .build()
                .unwrap();

            rt.block_on(async {
                // start_ping_agent(c.ticker_sec, c.ping_file.as_str()).await;
            });
        });
        let cfg = get_config().unwrap();
        let task_curl_handle = thread::spawn(move || {
            let c = cfg.clone();
            let rt = runtime::Builder::new_multi_thread()
                .worker_threads(c.threads)
                .enable_io()
                .enable_time()
                .build()
                .unwrap();
            rt.block_on(async {});
        });
        task_ping_handle.join().unwrap();
        task_curl_handle.join().unwrap();
    }

    if let Some(ref _matches) = matches.subcommand_matches("stop") {
        println!("server stopping...");
        // let sys = System::new_with_specifics(RefreshKind::with_processes(Default::default()));
        let r = RefreshKind::new();
        let r = r.with_processes(ProcessRefreshKind::everything());
        let sys = System::new_with_specifics(r);

        let pidstr = String::from_utf8(fs::read("pid").unwrap()).unwrap();
        let pid = Pid::from_str(pidstr.as_str()).unwrap();

        if let Some(p) = sys.process(pid) {
            println!("terminal process: {:?}", p.pid());
        } else {
            println!("Server not run!");
            return;
        };
        Command::new("kill")
            .args(["-15", pidstr.as_str()])
            .output()
            .expect("failed to execute process");
    }

    if let Some(config) = matches.subcommand_matches("config") {
        if let Some(_show) = config.subcommand_matches("show") {
            let yml = get_current_config_yml();
            match yml {
                Ok(str) => {
                    println!("{}", str);
                }
                Err(e) => {
                    eprintln!("{}", e);
                }
            }
        }

        if let Some(gen_config) = config.subcommand_matches("gendefault") {
            let mut file = String::from("");
            if let Some(path) = gen_config.get_one::<String>("filepath") {
                file.push_str(path);
            } else {
                file.push_str("config_default.yml")
            }
            if let Err(e) = generate_default_config(file.as_str()) {
                log::error!("{}", e);
                return;
            };
            println!("{} created!", file);
        }
    }

    if let Some(osstask) = matches.subcommand_matches("osstask") {
        if let Some(transfer) = osstask.subcommand_matches("transfer") {}
        if let Some(download) = osstask.subcommand_matches("download") {
            if let Some(f) = download.get_one::<String>("filepath") {
                let download = read_yaml_file::<TaskDownload>(f);

                let rt = tokio::runtime::Runtime::new().unwrap();
                let async_req = async {
                    match download {
                        Ok(d) => {
                            let c = d.source.gen_oss_client();
                            // 生成文件清单，文件清单默认文件存储在文件存储目录下 .objlist
                            let object_list_file = d.local_path.clone() + "/.objlist";
                            let _ = fs::remove_file(object_list_file.clone());
                            let r = c
                                .jd_object_list_to_file(
                                    None,
                                    d.source.bucket.clone(),
                                    object_list_file.clone(),
                                    d.bach_size,
                                    None,
                                )
                                .await;
                            println!("{:?}", r);

                            // 根据清单下载文件
                            let lines = read_lines(object_list_file.clone()).unwrap();
                            for line in lines {
                                if let Ok(f) = line {
                                    let r = c
                                        .jd_download_object_to_dir(
                                            d.source.bucket.clone(),
                                            f.clone(),
                                            d.local_path.clone(),
                                        )
                                        .await;
                                    println!("{:?}", r);
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("{}", e.to_string())
                        }
                    }
                };
                rt.block_on(async_req);
            }
        }

        if let Some(template) = osstask.subcommand_matches("template") {
            let task_id = task_id_generator();
            if let Some(_) = template.subcommand_matches("download") {
                let task_download = TaskDownload {
                    task_id: task_id.to_string(),
                    description: "download task".to_string(),
                    source: OSSDescription::default(),
                    local_path: "/tmp".to_string(),
                    bach_size: 100,
                };
                let yml = struct_to_yaml_string(&task_download);
                match yml {
                    Ok(str) => println!("{}", str),
                    Err(e) => eprintln!("{}", e.to_string()),
                }
            }

            if let Some(_) = template.subcommand_matches("transfer") {
                let task_id = task_id_generator();
                let mut oss_ali = OSSDescription::default();
                oss_ali.provider = OssProvider::Ali;
                oss_ali.endpoint = "oss-cn-beijing.aliyuncs.com".to_string();

                let task_transfer = TaskTransfer {
                    task_id: task_id.to_string(),
                    description: "transfer task".to_string(),
                    source: oss_ali,
                    target: OSSDescription::default(),
                    bach_size: 100,
                };
                let yml = struct_to_yaml_string(&task_transfer);
                match yml {
                    Ok(str) => println!("{}", str),
                    Err(e) => eprintln!("{}", e.to_string()),
                }
            }
        }
    }

    if let Some(osscfg) = matches.subcommand_matches("osscfg") {
        if let Some(gen) = osscfg.subcommand_matches("gendefault") {
            println!("gen osscfg file");
            let mut file = String::from("");
            if let Some(path) = gen.get_one::<String>("filepath") {
                file.push_str(path);
            } else {
                file.push_str("osscfg_default.yml")
            }
            let oss_jd = OSSDescription::default();
            let mut oss_ali = OSSDescription::default();
            oss_ali.provider = OssProvider::Ali;
            oss_ali.endpoint = "oss-cn-beijing.aliyuncs.com".to_string();
            let vec_oss = vec![oss_jd, oss_ali];
            if let Err(e) = struct_to_yml_file(&vec_oss, file.as_str()) {
                log::error!("{}", e);
                return;
            };
            println!("{} created!", file);
        }
    }
}
