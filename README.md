# 2017-highload-kv
Курсовой проект 2017 года [курса](https://polis.mail.ru/curriculum/program/discipline/50/) "Проектирование высоконагруженных систем" в [Технополис](https://polis.mail.ru).

## Этап 1. HTTP + storage (deadline 2017-10-10)
### Fork
[Форкните проект](https://help.github.com/articles/fork-a-repo/), склонируйте и добавьте `upstream`:
```
$ git clone git@github.com:<username>/2017-highload-kv.git
Cloning into '2017-highload-kv'...
remote: Counting objects: 34, done.
remote: Compressing objects: 100% (24/24), done.
remote: Total 34 (delta 2), reused 34 (delta 2), pack-reused 0
Receiving objects: 100% (34/34), 11.43 KiB | 3.81 MiB/s, done.
Resolving deltas: 100% (2/2), done.
$ git remote add upstream git@github.com:polis-mail-ru/2017-highload-kv.git
$ git fetch upstream
From github.com:polis-mail-ru/2017-highload-kv
 * [new branch]      master     -> upstream/master
```

### Make
Так можно запустить тесты:
```
$ gradle test
```

А вот так -- сервер:
```
$ gradle run
```

### Develop
Откройте в IDE -- [IntelliJ IDEA Community Edition](https://www.jetbrains.com/idea/) нам будет достаточно.

**ВНИМАНИЕ!** При запуске тестов или сервера в IDE необходимо передавать Java опцию `-Xmx1g`. 

В своё Java package `ru.mail.polis.<username>` реализуйте интерфейс [`KVService`](src/main/java/ru/mail/polis/KVService.java) и поддержите следующий HTTP REST API протокол:
* HTTP `GET /v0/entity?id=<ID>` -- получить данные по ключу `<ID>`. Возвращает `200 OK` и данные или `404 Not Found`.
* HTTP `PUT /v0/entity?id=<ID>` -- создать/перезаписать (upsert) данные по ключу `<ID>`. Возвращает `201 Created`.
* HTTP `DELETE /v0/entity?id=<ID>` -- удалить данные по ключу `<ID>`. Возвращает `202 Accepted`.

Возвращайте реализацию интерфейса в [`KVServiceFactory`](src/main/java/ru/mail/polis/KVServiceFactory.java#L48).

Продолжайте запускать тесты и исправлять ошибки, не забывая [подтягивать новые тесты и фиксы из `upstream`](https://help.github.com/articles/syncing-a-fork/). Если заметите ошибку в `upstream`, заводите баг и присылайте pull request ;)

### Report
Когда всё будет готово, присылайте pull request со своей реализацией на review. Не забывайте **отвечать на комментарии в PR** и **исправлять замечания**!

## Этап 2. TBD (deadline 2017-11-07)

## Этап 3. TBD (deadline 2017-12-05)
