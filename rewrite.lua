function rewrite(a)
    if a:find("bar") then 
        error("I don't like bar")
    end
	return a:gsub("foo", "baz")
end