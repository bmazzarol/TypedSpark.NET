<!-- markdownlint-disable MD033 MD041 -->
<div align="center">

<img src="typewriter-icon.png" alt="TypedSpark.NET" width="150px"/>

# TypedSpark.NET

> Due to inactivity and lack of support for [spark.NET](https://github.com/dotnet/spark)
> this has been archived.
> I would recommend building Spark applications in a supported language, not
> in dotnet.

[:running: **_Getting Started_**](https://bmazzarol.github.io/TypedSpark.NET/articles/getting-started.html)
[:books: **_Documentation_**](https://bmazzarol.github.io/TypedSpark.NET)

[![Nuget](https://img.shields.io/nuget/v/TypedSpark.NET)](https://www.nuget.org/packages/TypedSpark.NET/)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=bmazzarol_TypedSpark.NET&metric=coverage)](https://sonarcloud.io/summary/new_code?id=bmazzarol_TypedSpark.NET)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=bmazzarol_TypedSpark.NET&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=bmazzarol_TypedSpark.NET)
[![CD Build](https://github.com/bmazzarol/TypedSpark.NET/actions/workflows/cd-build.yml/badge.svg)](https://github.com/bmazzarol/TypedSpark.NET/actions/workflows/cd-build.yml)
[![Check Markdown](https://github.com/bmazzarol/TypedSpark.NET/actions/workflows/check-markdown.yml/badge.svg)](https://github.com/bmazzarol/TypedSpark.NET/actions/workflows/check-markdown.yml)
[![CodeQL](https://github.com/bmazzarol/TypedSpark.NET/actions/workflows/codeql.yml/badge.svg)](https://github.com/bmazzarol/TypedSpark.NET/actions/workflows/codeql.yml)

Typesafe bindings for :star: Spark.NET

</div>

> **_IMPORTANT:_** Please note this library under active construction
> :construction_worker: and should not be used in production. Help is always
> appreciated, create an issue, check the code out and have a play!

## Features

* Check Spark programs at compile time
* Zero dependencies (except spark dotnet!)
* Easy to use, its LINQ for Spark
* Replace stringly typed code with strong models
* No more APIs untyped Spark APIs

```c#
// create a model using typed columns
public sealed Person: TypedSchema<Person>
{
    public StringColumn Name { get; private set; }
    public IntegerColumn Age { get; private set; }
    
    public Person(string? alias): base(alias) {}
    public Person(): this(default) {}
}

// now it can be used in typed query operations using LINQ
DataFrame df;
TypedDataFrame<Person> personDf = df.AsTyped<Person>();

personDf
    .Where(x => x.Age > 18)
    .OrderBy(x => new { Age = x.Age.Desc() })
    .Select(x => new { PersonName = x.Name });

// more to come!!
```

## Getting Started

Coming Soon.

## Why?

Strong types facilitate better code, Spark is typed, leveraging the C# type
system we can expose those types and enforce correct Spark applications before
they are even run.

More details to come!

## Attributions

[Fire icons created by juicy_fish](https://www.flaticon.com/free-icons/fire)
