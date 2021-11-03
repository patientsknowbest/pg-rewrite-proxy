function rewrite(a)
    return a:gsub("current_timestamp", "now()")
end 

-- Called when a simple Query message is received
function rewriteQuery(a)
    return rewrite(a)
end

-- Called with extended query is submitted
function rewriteParse(a)
    return rewrite(a)
end
