<!-- markdownlint-disable MD033 MD041 -->
<div align="center">

<img src="typewriter-icon.png" alt="TypedSpark.NET" width="150px"/>

# TypedSpark.NET

[![Nuget](https://img.shields.io/nuget/v/TypedSpark.NET)](https://www.nuget.org/packages/TypedSpark.NET/)
[![Coverage Status](https://coveralls.io/repos/github/bmazzarol/TypedSpark.NET/badge.svg?branch=main)](https://coveralls.io/github/bmazzarol/TypedSpark.NET?branch=main)
[![CD Build](https://github.com/bmazzarol/TypedSpark.NET/actions/workflows/cd-build.yml/badge.svg)](https://github.com/bmazzarol/TypedSpark.NET/actions/workflows/cd-build.yml)
[![Check Markdown](https://github.com/bmazzarol/TypedSpark.NET/actions/workflows/check-markdown.yml/badge.svg)](https://github.com/bmazzarol/TypedSpark.NET/actions/workflows/check-markdown.yml)
[![CodeQL](https://github.com/bmazzarol/TypedSpark.NET/actions/workflows/codeql.yml/badge.svg)](https://github.com/bmazzarol/TypedSpark.NET/actions/workflows/codeql.yml)

Typesafe bindings for :star: Spark.NET

</div>

> **_IMPORTANT:_** Please note this library under active construction :
> construction_worker: and should not be used in production. Help is always
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
