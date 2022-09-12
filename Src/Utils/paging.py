from math import floor

def get_page_by_offset(offset, rows_per_page):
    return (offset / rows_per_page) + 1

def get_skip_items_by_offset(page, rows_per_page):
    skip = page * rows_per_page; 
    return skip

def has_prev(page):
    return page > 1

def has_next(page, total_rows, rows_per_page):
    last_page = floor(total_rows / rows_per_page) + (total_rows % rows_per_page);
    return True if page != last_page else False