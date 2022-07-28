window.localStorage['page'] = 1;
document.querySelector(".btn-preview").style.display = "none";
document.querySelector(".btn-next").style.display = "none";

function getPage(flag) {
    if (flag) {
        window.localStorage['page'] = parseInt(window.localStorage['page']) + 1;
        document.querySelector(".btn-preview").style.display = "inline-block";
    } else {
        window.localStorage['page'] = parseInt(window.localStorage['page']) - 1;
        if (window.localStorage['page'] == 1) {
            document.querySelector(".btn-preview").style.display = "none";
        }
    }
    sendRequest(parseInt(window.localStorage['page']), false);
}

function sendRequest(page, flag) {
    if (flag) {
        window.localStorage['page'] = 1;
        document.querySelector(".btn-preview").style.display = "none";
    }
    document.querySelector('.data').innerHTML = "";
    let spinner = document.createElement("div");
    spinner.classList.add("spinner");
    spinner.setAttribute("id", "spinner")
    document.querySelector(".requestForm").append(spinner);

    let data = {
        'dbtype': '',
        'mainfilter': {
            'Child': 0,
        },
        'page': page,
    }

    let result = getInputValue(data)

    fetch(`http://46.48.3.74:8000/` + "getdeepdata", {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json;charset=utf-8'
        },
        body: JSON.stringify(result)
    })
        .then(response => {
            if (response.ok) {
                return response.json()
            }
        })
        .then(data => {
            let selectDB = document.querySelector(".select-db");

            if (selectDB.options[selectDB.selectedIndex].value === "PostgreSQL") {
                parseData(data, 'pk', 'face_type', 'face_name')
            } else if (selectDB.options[selectDB.selectedIndex].value === "Neo4j") {
                parseData(data, 'pk', 'face_type', 'face_name')
            } else if (selectDB.options[selectDB.selectedIndex].value === "OrientDB") {
                parseData(data, 'id', 'face_type', 'name')
            }

            document.querySelector(".btn-next").style.display = "inline-block";
        });
}

function parseData(data, id_name, type_name, name_name) {
    let result = data['result']
    let time_block = document.createElement("div");
    time_block.classList.add("time");
    time_block.innerHTML = `Найдено за ${data['time']} сек`;
    document.querySelector('.data').append(time_block);

    let records_container = document.createElement("div");
    records_container.classList.add("deep-records-container");

    for (let record in result) {
        let record_container = document.createElement("div")
        record_container.classList.add("deep-record-container");

        for (let i = 0; i < result[record].length; i++) {
            if (i === 0) {
                let child = document.createElement("div")
                child.classList.add("deep-child-container")
                child.innerHTML = `
                        <div class="deep-child-item">Face_id: ${result[record][i][id_name]}</div>
                        <div class="deep-child-item">Face_name: ${result[record][i][name_name]}</div>
                        <div class="deep-child-item">Face_type: ${result[record][i][type_name]}</div>
                     `
                record_container.append(child)
            } else {
                let parent = document.createElement("div")
                parent.classList.add("deep-parent-container")
                parent.innerHTML = `
                        <div class="deep-parent-item">Face_id: ${result[record][i][id_name]}</div>
                        <div class="deep-parent-item">Face_name: ${result[record][i][name_name]}</div>
                        <div class="deep-parent-item">Face_type: ${result[record][i][type_name]}</div>
                     `
                record_container.append(parent);
            }
        }
        records_container.append(record_container);
    }
    document.querySelector('.data').append(records_container)
    document.getElementById("spinner").remove();
}

function getInputValue(data) {
    let result = data;
    let selectDB = document.querySelector(".select-db");

    result['dbtype'] = selectDB.options[selectDB.selectedIndex].value;
    result['mainfilter']['Child'] = document.querySelector("#child").value;

    return result
}