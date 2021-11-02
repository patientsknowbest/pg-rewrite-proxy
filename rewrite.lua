function rewrite(a)
	return a:gsub("current_timestamp", "now()")
end