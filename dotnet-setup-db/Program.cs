using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http.Headers;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;


namespace dotnet_setup_db
{
    class Program
    {
        static void Help(string error)
        {
            Console.Error.WriteLine(
                @"Database preparation tool.
");

            if (error != null)
                Console.Error.WriteLine("Error: {0}", error);
            Console.Error.WriteLine(
                @"
Usage:
dotnet setup-db option1 option2 ... key1=value1 key2=value2 ...

Options:
Full name           Shorthand      Description
assembly            asm, a         [Mandatory] Database driver assembly reference.
nuget               n              [Mandatory] Database driver nuget package name.
type                t              [Mandatoty] Database driver full type name.
connect             c, s           [Mandatory] Database connection string. It must allow connecting to DBMS without existing database.
script                             [Optional]  Database script file name. Defaults to create.sql.
single-line                        [Optional]  Run every single line of the script in separate command.
create                             [Optional]  Create new database before executing. This is default.
nocreate                           [Optional]  Do not create new database before executing.
transaction         tx             [Optional]  Wrap script into transaction. This is default.
notransaction       notx           [Optional]  Do not wrap script into transaction.
drop                               [Optional]  Drop database specified with agrument. This options prevents script from running.
pkgpath             pkg, p         [Optional]  Path to directory to use for nuget package storage. Defaults to .pkg, needed only when -nuget is used.
");
        }

        static int Main(string[] args)
        {
            string assembly = null, nuget = null, type = null, cstring = null, script = "create.sql", drop = null, pkgPath = ".pkg";
            bool create = true, tx = true, singleLine=false;
            for (var i = 0; i < args.Length; i++)
            {
                switch (args[i].ToLower().TrimStart('-', '/', '\\'))
                {
                    case "n":
                    case "nuget":
                        if (args.Length == i + 1)
                        {
                            Help($"You must specify nuget package name after {args[i]} parameter");
                            return -2;
                        }
                        nuget = args[i + 1];
                        i++;
                        break;
                    case "a":
                    case "asm":
                    case "assembly":
                        if (args.Length == i + 1)
                        {
                            Help($"You must specify assembly reference after {args[i]} parameter");
                            return -2;
                        }
                        assembly = args[i + 1];
                        i++;
                        break;
                    case "t":
                    case "type":
                        if (args.Length == i + 1)
                        {
                            Help($"You must specify full type name after {args[i]} parameter");
                            return -2;
                        }
                        type = args[i + 1];
                        i++;
                        break;
                    case "drop":
                        if (args.Length == i + 1)
                        {
                            Help($"You must specify database name after {args[i]} parameter");
                            return -2;
                        }
                        drop = args[i + 1];
                        i++;
                        break;
                    case "pkgpath":
                    case "pkg":
                    case "p":
                    case "pkg-path":
                        if (args.Length == i + 1)
                        {
                            Help($"You must specify directory after {args[i]} parameter");
                            return -2;
                        }
                        pkgPath = args[i + 1];
                        i++;
                        break;
                    case "s":
                    case "c":
                    case "connect":
                    case "connection":
                    case "connectionstring":
                    case "connection-string":
                        if (args.Length == i + 1)
                        {
                            Help($"You must specify connection string after {args[i]} parameter");
                            return -2;
                        }
                        cstring = args[i + 1];
                        i++;
                        break;
                    case "script":
                        if (args.Length == i + 1)
                        {
                            Help($"You must specify script path after {args[i]} parameter");
                            return -2;
                        }
                        script = args[i + 1];
                        i++;
                        break;
                    case "create":
                        create = true;
                        break;
                    case "single-line":
                        singleLine = true;
                        break;
                    case "nocreate":
                    case "no-create":
                        create = false;
                        break;
                    case "tx":
                    case "transaction":
                        tx = true;
                        break;
                    case "notx":
                    case "no-tx":
                    case "no-transaction":
                    case "notransaction":
                        tx = false;
                        break;
                    default:
                        var kval = args[i].Split(new[] {'='}, 2);
                        if (kval.Length != 2)
                        {
                            Help("Unknown option " + args[i]);
                            return -2;
                        }
                        break;
                }
            }

            if (!Directory.Exists(pkgPath))
                Directory.CreateDirectory(pkgPath);

            if (!string.IsNullOrEmpty(nuget))
            {
                var rv = ResolveNugetPackage(nuget, null, pkgPath);
                rv.Wait();
                assembly = rv.Result;
            }
            if (string.IsNullOrEmpty(assembly))
            {
                Help("Assembly reference must be specified.");
                return -2;
            }
            if (!File.Exists(assembly))
            {
                Help("Assembly file does not exist.");
                return -2;
            }
            if (string.IsNullOrEmpty(type))
            {
                Help("Driver type must be specified.");
                return -2;
            }
            if (string.IsNullOrEmpty(cstring))
            {
                Help("Connection string must be specified.");
                return -2;
            }
            if (!File.Exists(script) && string.IsNullOrEmpty(drop))
            {
                Help($"Script file {script} does not exist.");
                return -4;
            }
            AppDomain.CurrentDomain.AssemblyResolve += (sender, evArgs) =>
            {
                var f = Path.GetFullPath(Path.Combine(pkgPath, new AssemblyName(evArgs.Name).Name + ".dll"));
                if (File.Exists(f))
                    return Assembly.LoadFile(f);
                return null;
            };
            Console.Error.WriteLine("Creating driver instance");
            var asm = Assembly.LoadFile(Path.GetFullPath(assembly));
            var drv = asm.GetType(type);
            using (var cnn = (IDbConnection) Activator.CreateInstance(drv))
            {
                cnn.ConnectionString = cstring;
                cnn.Open();
                if (drop != null)
                {
                    Console.Error.WriteLine($"Dropping database {drop}");
                    using (var dropCmd = cnn.CreateCommand())
                    {
                        if (asm.GetName().Name == "Npgsql")
                        {
                            Console.Error.WriteLine($"Terminating backends for {drop}");
                            dropCmd.CommandText = "SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = @dbname AND pid <> pg_backend_pid();";
                            var p = dropCmd.CreateParameter();
                            p.ParameterName = "dbname";
                            p.DbType = DbType.String;
                            p.Value = drop;
                            dropCmd.Parameters.Add(p);
                            dropCmd.ExecuteNonQuery();
                        }
                        dropCmd.CommandText = "DROP DATABASE " + drop;
                        dropCmd.ExecuteNonQuery();
                    }
                }
                else
                {
                    if (create)
                    {
                        var dbName = "autotest_" + Guid.NewGuid().ToString("N").Substring(0, 8);
                        Console.Error.WriteLine($"Creating database {dbName}");
                        using (var createCmd = cnn.CreateCommand())
                        {
                            createCmd.CommandText = "CREATE DATABASE " + dbName;
                            createCmd.ExecuteNonQuery();
                        }
                        cnn.ChangeDatabase(dbName);
                    }

                    IDbTransaction txn = null;
                    if (tx)
                    {
                        Console.Error.WriteLine("Entering transaction");
                        txn = cnn.BeginTransaction();
                    }

                    if (singleLine)
                    {
                        var scriptData = File.ReadAllLines(script);
                        using (var cmd = cnn.CreateCommand())
                        {
                            foreach (var line in scriptData)
                            {
                                cmd.CommandText = line;
                                cmd.ExecuteNonQuery();
                            }
                        }
                    }
                    else
                    {
                        var scriptData = File.ReadAllText(script);
                        using (var cmd = cnn.CreateCommand())
                        {
                            cmd.CommandText = scriptData;
                            cmd.ExecuteNonQuery();
                        }
                    }

                    if (tx && txn != null)
                    {
                        txn.Commit();
                        txn.Dispose();
                    }

                    Console.WriteLine(cnn.Database);
                }
            }
            return 0;
        }

        private static async Task<string> ResolveNugetPackage(string nuget, string ver, string pkgPath)
        {
            Console.Error.WriteLine($"Resolving package {nuget} {ver}");
            var dlNuspecName = Path.Combine(pkgPath, $"{nuget}.{ver}.nuspec");
            byte[] nuspecData;
            string nugetUrl = null;
            var wc = new WebClient();
            if (!File.Exists(dlNuspecName))
            {
                string nuspecUrl;
                if (string.IsNullOrEmpty(ver))
                {
                    Console.Error.WriteLine($"Requesting latest version of {nuget}");
                    var versionInfo = (JObject) JsonConvert.DeserializeObject(Encoding.UTF8.GetString(await wc.DownloadDataTaskAsync($"https://api.nuget.org/v3/registration3/{nuget.Trim().ToLower()}/index.json")));
                    var packageRef =
                        ((JArray) versionInfo["items"]?.FirstOrDefault()?["items"])
                        ?.Cast<JObject>().OrderByDescending(x =>
                        {
                            var xversion = x["catalogEntry"]?["version"].Value<string>();
                            if (xversion == null) return new Version();
                            return new Version(xversion.Split('-')[0]);
                        })
                        .FirstOrDefault();
                    if (packageRef == null)
                    {
                        Help($"Cannot find latest version of nuget package {nuget}");
                        return null;
                    }
                    nugetUrl = packageRef["packageContent"].Value<string>();
                    ver = packageRef["catalogEntry"]?["version"]?.Value<string>();
                    nuspecUrl = $"https://api.nuget.org/v3-flatcontainer/{nuget}/{ver}/{nuget}.nuspec";
                }
                else
                {
                    nugetUrl = $"https://api.nuget.org/v3-flatcontainer/{nuget}/{ver}/{nuget}.{ver}.nupkg";
                    nuspecUrl = $"https://api.nuget.org/v3-flatcontainer/{nuget}/{ver}/{nuget}.nuspec";
                }

                Console.Error.WriteLine($"Downloading nuspec for package {nuget}");
                nuspecData = await wc.DownloadDataTaskAsync(nuspecUrl);
                File.WriteAllBytes(dlNuspecName, nuspecData);
            }
            else
            {
                nuspecData = File.ReadAllBytes(dlNuspecName);
            }
            var nuspec = XDocument.Parse(Encoding.UTF8.GetString(nuspecData));
            var nugetNs = nuspec.Root.Name.Namespace.NamespaceName;
            if (nugetUrl == null)
            {
                ver = nuspec.Descendants(XName.Get("version", nugetNs)).FirstOrDefault()?.Value ?? ver;
                nugetUrl = $"https://api.nuget.org/v3-flatcontainer/{nuget}/{ver}/{nuget}.{ver}.nupkg";
            }
            var refs = nuspec.Descendants(XName.Get("reference", nugetNs)).Select(x => x.Attribute("file")?.Value).Where(x => x != null).ToArray();
            if (!refs.Any()) refs = new[] {nuget + ".dll"};
            if (!refs.All(x => File.Exists(Path.Combine(pkgPath, x))))
            {
                Console.Error.WriteLine($"Downloading package {nuget}");
                var nupkg = new ZipArchive(new MemoryStream(await wc.DownloadDataTaskAsync(nugetUrl)));
                var mydll = nupkg.Entries.First(x => (x.FullName.StartsWith("lib/netstandard") || x.FullName.StartsWith("lib/netcoreapp")) && x.Name.EndsWith(".dll"));
                if (!Directory.Exists(Path.GetDirectoryName(dlNuspecName)))
                    Directory.CreateDirectory(Path.GetDirectoryName(dlNuspecName));
                using (var fs = File.Create(Path.Combine(pkgPath,mydll.Name)))
                    mydll.Open().CopyTo(fs);
            }
            var deps = nuspec.Descendants(XName.Get("dependencies", nugetNs));
            var groups = deps.SelectMany(x => x.Elements(XName.Get("group", nugetNs)))
                .Where(x => !(x.Attribute(XName.Get("targetFramework"))?.Value.StartsWith(".NETStandard") ?? false));
            var d = groups
                .SelectMany(x => x.Elements(XName.Get("dependency", nugetNs)));
            var dd = d
                .Select(x => new {Elem = x, Id = x.Attribute(XName.Get("id"))?.Value})
                .Where(x => !x.Id.StartsWith("System.") && x.Id != "NETStandard.Library" && x.Id != "Microsoft.NETCore.App");
            Task.WaitAll(dd.Select(x => ResolveNugetPackage(x.Id, x.Elem.Attribute(XName.Get("version"))?.Value, pkgPath)).Cast<Task>().ToArray());
            return Path.Combine(pkgPath, refs.First());
        }
    }
}