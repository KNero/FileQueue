FileQueue
=========
## Maven
```xml
<repositories>
    <repository>
        <id>knero-mvn-repo</id>
        <url>https://github.com/KNero/repository/raw/master/maven</url>
    </repository>
</repositories>
```
```xml
<dependency>
    <groupId>team.balam</groupId>
    <artifactId>file-queue</artifactId>
    <version>0.0.1</version>
</dependency>
```
## Gradle
```gradle
repositories {
    maven {
        mavenLocal()
        maven {
            url "https://github.com/KNero/repository/raw/master/maven"
        }
    }
}
```
```gradle
dependencies {
    compile 'team.balam:file-queue:0.0.1'
}
```
