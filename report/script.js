function isTableId(id) {
    return /^result-table-row-\d*$/.test(id);
}

function hide_row(row_even){
    const element = row_even.getAttribute("id")
    if (isTableId(element)){
        const row_number = Number(element.split("-").at(-1))
        if (row_number % 2 == 0){
            const row_odd = document.getElementById("result-table-row-" + Number(row_number + 1))
            if (row_odd.getAttribute("class") != "row-hide"){
                row_odd.setAttribute("class", "row-hide");
                row_even.setAttribute("style", "Null");
            } else {
                row_odd.setAttribute("class", "Null");
                row_even.setAttribute("style", "border-left: 5px solid green;");
            }
        }
    }
}

function button(target){
    const element = target.getAttribute("id")
    if (element == "main-menu"){
        span_text = document.getElementById("main-menu")
        if (span_text.textContent == "Expand"){
            span_text.textContent = "Collapse";
            odd_rows = document.getElementsByClassName("result-table")[0].rows.length-1;
            for (let i = odd_rows; i > 0; i = i - 2) {
                row_odd = document.getElementById("result-table-row-" + Number(i))
                row_odd.setAttribute("class", "Null");
                row_even = document.getElementById("result-table-row-" + Number(i - 1))
                row_even.setAttribute("style", "border-left: 5px solid green;");
            }
        } else {
            span_text.textContent = "Expand";
            odd_rows = document.getElementsByClassName("result-table")[0].rows.length-1;
            for (let i = odd_rows; i > 0; i = i - 2) {
                row_odd = document.getElementById("result-table-row-" + Number(i))
                row_odd.setAttribute("class", "row-hide");
                row_even = document.getElementById("result-table-row-" + Number(i - 1))
                row_even.setAttribute("style", "Null");
            }
        }
    }
}

document.addEventListener('click', function(e) {
    e = e || window.event;
    var target = e.target;
    hide_row(target);
    button(target);
}, false);