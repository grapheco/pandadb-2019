Rule 1: Make sure your code has no side-effect!!!!
> do not use `println()`

Rule 2: Logging: use Logging trait
>
> ```
class YourClass extends cn.graiph.util.Logging {
  logger.debug(s"hello...")
}
> ```
> Yes! do not use println()!

Rule 3: Do not keep useless comments!!!
> wrong comments/validated comments will lead people to a wrong way!

Rule 4: use s"" to format string
> use: `s"hello, $name"`, do not use: `"hello, "+name`
