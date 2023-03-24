using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Microsoft.Spark.Sql;

namespace SparkTest.NET;

/// <summary>
/// Provides access to spark sessions backed by a running spark-debug process.
/// </summary>
[ExcludeFromCodeCoverage]
[SuppressMessage("Minor Code Smell", "S3963:\"static\" fields should be initialized inline")]
public static class SparkSessionFactory
{
    private static readonly ConcurrentDictionary<string, Lazy<SparkSession>> SparkSessions;

    /// <summary>
    /// Singleton shared spark session
    /// </summary>
    public static readonly SparkSession DefaultSession;

    static SparkSessionFactory()
    {
        SparkSessions = new ConcurrentDictionary<string, Lazy<SparkSession>>(
            StringComparer.CurrentCultureIgnoreCase
        );
        // start the spark-debug process
        var process = TryStartSparkDebug();
        // set default session
        DefaultSession = GetOrCreateSession(nameof(SparkSessionFactory));
        // shutdown hook
        AppDomain.CurrentDomain.ProcessExit += (_, _) =>
        {
            foreach (var kvp in SparkSessions.Where(x => x.Value.IsValueCreated))
                kvp.Value.Value.Stop();

            if (process == null)
                return;

            // CSparkRunner will exit upon receiving newline from
            // the standard input stream.
            process.StandardInput.WriteLine("done");
            process.StandardInput.Flush();
            process.WaitForExit();
        };
    }

    /// <summary>
    /// Gets or creates a new session
    /// </summary>
    /// <param name="appName">application name</param>
    /// <returns>spark session</returns>
    public static SparkSession GetOrCreateSession(string appName) =>
        SparkSessions
            .GetOrAdd(
                appName,
                _ =>
                    new Lazy<SparkSession>(
                        () =>
                            SparkSession
                                .Builder()
                                .Config("spark.sql.session.timeZone", "UTC")
                                .Config("spark.ui.enabled", false)
                                .Config("spark.ui.showConsoleProgress", false)
                                .AppName(appName)
                                .GetOrCreate()
                    )
            )
            .Value;

    private static Process? TryStartSparkDebug()
    {
        // if we are running in a CI env dont try start spark-submit
        if (
            string.Equals(
                Environment.GetEnvironmentVariable("DISABLE_AUTO_SPARK_DEBUG"),
                "true",
                StringComparison.OrdinalIgnoreCase
            )
        )
        {
            return null;
        }

        // if the user already has spark running lets not try and start it
        try
        {
            _ = GetOrCreateSession("test");
            return null;
        }
        catch
        {
            // no-op
        }

        if (
            !(
                Environment.GetEnvironmentVariable("SPARK_HOME") is { } sparkHome
                && !string.IsNullOrEmpty(sparkHome)
            )
        )
        {
            throw new InvalidOperationException("Environment variable 'SPARK_HOME' must be set.");
        }

        if (
            !(
                Environment.GetEnvironmentVariable("DOTNET_WORKER_DIR") is { } dotnetWorkerDir
                && !string.IsNullOrEmpty(dotnetWorkerDir)
            )
        )
        {
            throw new InvalidOperationException(
                "Environment variable 'DOTNET_WORKER_DIR' must be set."
            );
        }

        return Process(
            sparkHome,
            Environment.GetEnvironmentVariable("SPARK_DOTNET_JAR_NAME")
                ?? throw new InvalidOperationException(
                    "SPARK_DOTNET_JAR_NAME environment variable is not set. This drives what version of spark dotnet is used with spark-debug"
                ),
            Environment.GetEnvironmentVariable("SPARK_DEBUG_EXTRA_JARS")
        );
    }

    private static Process Process(string sparkHome, string sparkJarName, string? extraJars)
    {
        var process = new Process();
        process.StartInfo.FileName =
            Path.Combine(sparkHome, "bin", "spark-submit")
            + (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? ".cmd" : string.Empty);
        process.StartInfo.WorkingDirectory = Directory.GetCurrentDirectory();

        WriteLogConfiguration(process);

        process.StartInfo.Arguments = string.Join(
            " ",
            "--class org.apache.spark.deploy.dotnet.DotnetRunner",
            string.IsNullOrWhiteSpace(extraJars) ? string.Empty : $"--jars {extraJars}",
            "--conf \"spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties\"",
            "--conf \"spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties\"",
            "--conf \"spark.deploy.spreadOut=false\"",
            "--conf \"spark.shuffle.service.db.enabled=false\"",
            "--conf \"spark.sql.shuffle.partitions=1\"",
            $"--master local[*] {sparkJarName} debug"
        );
        // UseShellExecute defaults to true in .NET Framework,
        // but defaults to false in .NET Core. To support both, set it
        // to false which is required for stream redirection.
        process.StartInfo.UseShellExecute = false;
        process.StartInfo.RedirectStandardInput = true;
        process.StartInfo.RedirectStandardOutput = true;
        process.StartInfo.RedirectStandardError = true;

        var isSparkReady = false;
        process.OutputDataReceived += (sender, arguments) =>
        {
            // Scala-side driver for .NET emits the following message after it is
            // launched and ready to accept connections.
            if (!isSparkReady && arguments.Data?.Contains("Backend running debug mode") == true)
            {
                isSparkReady = true;
            }
        };

        process.Start();
        process.BeginErrorReadLine();
        process.BeginOutputReadLine();

        var processExited = false;
        while (!isSparkReady && !processExited)
        {
            processExited = process.WaitForExit(500);
        }

        if (!processExited)
            return process;

        process.Dispose();

        // The process should not have been exited.
        throw new InvalidOperationException(
            $"Process exited prematurely with '{process.StartInfo.FileName} {process.StartInfo.Arguments}' in working directory '{process.StartInfo.WorkingDirectory}'"
        );
    }

    private static void WriteLogConfiguration(Process process) =>
        File.WriteAllText(
            Path.Combine(process.StartInfo.WorkingDirectory, "log4j.properties"),
            @"
log4j.rootCategory=ERROR, console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n"
        );
}
