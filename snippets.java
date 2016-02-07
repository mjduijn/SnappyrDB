// Deletion slide

snappyrdb.query()
.getKey(s -> true)
.extend(new DeleteFrom(snappyrdb))


snappyrdb.query()
.getKey(s -> s.contains("snappydb"))
.skip(2)
.take(5)
.extend(new DeleteFrom(snappyrdb))
.subscribe(
	(error) -> {..}, 
	() -> {..}
)


