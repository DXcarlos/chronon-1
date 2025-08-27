## useful scala commands
```
# reformat scala code
./mill __.reformat

# compile scala code
./mill __.compile

# run specific tests that match a pattern etc
./mill spark.test.testOnly "ai.chronon.spark.test.analyzer.*"

# run a particular test case inside a test class
./mill spark.test.testOnly "ai.chronon.spark.test.kv_store.KVUploadNodeRunnerTest" -- -z "should handle GROUP_BY_UPLOAD_TO_KV successfully"

# run all tests of a sub-module
./mill api.test

# to find where a dependency comes from
./mill spark.showMvnDepsTree --whatDependsOn com.google.protobuf:protobuf-java
```

## useful python commands
```
# reformat python code
./mill python.ruffCheck --fix

# run python tests
./mill python.test

# build the wheel - the user needs to have python installed
./mill python.wheel

# build and run the entry point (zipline.py)
./mill python.run hub backfill ...

# publish to pypi
# ask nikhil to generate a token for you
export MILL_TWINE_REPOSITORY_URL=https://pypi.org/
export MILL_TWINE_USERNAME=__token__
export MILL_TWINE_PASSWORD=<apitoken> 
./mill python.publish

# build self contained python binary - include python and all deps (transitive included) into a single file
# uses pex under the hood
./mill python.bundle
```

## project setup

Mill build spans across a central build.mill and a per module package.mill file.

Here is a simple but real example

```scala
object `package` extends build.BaseModule {
  def moduleDeps = Seq(build.api)

  // equivalent to "provided" , meaning the package is only available for compile
  // but at runtime it needs to come through from class path.
  def compileMvnDeps = Seq(
    mvn"org.apache.spark::spark-sql:${build.Constants.sparkVersion}"
  )
  
  // the actual dependencies
  def mvnDeps = build.Constants.commonDeps ++ build.Constants.loggingDeps ++ build.Constants.utilityDeps ++ Seq(
    mvn"org.apache.datasketches:datasketches-memory:3.0.2",
    mvn"org.apache.datasketches:datasketches-java:6.1.1",
  )
  
  // test setup
  object test extends build.BaseTestModule {
    def moduleDeps = Seq(build.aggregator, build.api.test)
    def mvnDeps = super.mvnDeps() ++ build.Constants.testDeps
  }
}
```

## why mill?

mill has a couple of nice side effects:
1. builds are very fast - apparently it changes the assembled jar surgically
  
   a. they are cached and executed in parallel
2. python support - comes with 

   a. ruff linting 
   b. pytest support 
   c. wheel 
   d. pex bundling 
   e. pypi uploads for free  (literally 20 lines of code to get all our py workflow into mill)

3. i can understand what is going on with the builds 

   a. mill is a significant reduction in code size and indirection 
   b. the ide click-into and auto complete works even with build files

4. the build output size for spark goes from 480MB to 62MB 

    a. simply because we can mark spark deps as “provided” that only are available for compilation but not for assembly or runtime.

