# weasel

weasel is a search engine implementation for the primary purpose of issuing complex Boolean queries.
Dissatisfied with existing search engines that are either too slow, cumbersome, or complicated, I wanted
to build something that just works well enough while still being able to understand everything if you
were to open up the source code and look for yourself.

I'm still playing around with efficiency so it's probably not a good idea to use this for anything
serious. However, I've tried to keep the code readable and pretty simple.

Right now, I'm building pretty much everything from scratch. The only real exception would be
[diskv](https://github.com/peterbourgon/diskv) for document persistence. Additionally, since I deal with
different types of queries, the query language is just [cqr](https://github.com/hscells/cqr).

Finally, weasel also has some associated binaries - `windex` and `wquery` for indexing documents and
querying documents respectively. These aren't finished either but they might give a decent example at
how to use weasel as a library.