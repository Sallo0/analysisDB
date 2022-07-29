function displayInput() {
    let selectBox = document.querySelector(".select-mainFilter");
    let selectedValue = selectBox.options[selectBox.selectedIndex].value;
    document.querySelector(".filter-input").style.display = "inline-block";

    let option = document.querySelector(".option-none");
    if (option != null) document.querySelector(".option-none").remove();

    if (selectedValue === "Child") {
        let parent = document.querySelector(".parent")
        document.querySelector(".parent-container").style.display = "none"
        document.querySelector(".child-container").style.display = "inline-block"
        parent.value = ""
    } else if (selectedValue === "Parent") {
        let child = document.querySelector(".child")
        document.querySelector(".child-container").style.display = "none"
        document.querySelector(".parent-container").style.display = "inline-block"
        child.value = ""
    } else if (selectedValue === "All") {
        document.querySelector(".child-container").style.display = "inline-block";
        document.querySelector(".parent-container").style.display = "inline-block";
    }
}

function getInputValue(data) {
    let result = data;
    let selectDB = document.querySelector(".select-db");
    let selectKind = document.querySelector(".select-kind");
    let selectChildLiq = document.querySelector(".select-childLiquidated");

    result['dbtype'] = selectDB.options[selectDB.selectedIndex].value;
    result['mainfilter']['Child'] = document.querySelector(".child").value;
    result['mainfilter']['Parent'] = document.querySelector(".parent").value;
    if ( parseInt(result['mainfilter']['Child']) == NaN && parseInt(result['mainfilter']['Parent']) == NaN){
        return false
    }
    result['kind'] = selectKind.options[selectKind.selectedIndex].value;
    result['date_begin'] = document.querySelector(".date-begin").value;
    result['date_end'] = document.querySelector(".date-end").value;
    result['cost'] = document.querySelector(".cost").value;
    result['share'] = document.querySelector(".share").value;
    result['child_liquidated'] = selectChildLiq.options[selectChildLiq.selectedIndex].value;

    return result
}

function sendRequest() {
    let spinner = document.createElement("div")
    spinner.classList.add("spinner");
    spinner.setAttribute("id", "spinner")
    document.querySelector(".requestForm").append(spinner)

    let data = {
        'dbtype': '',
        'mainfilter': {
            'Child': 0,
            'Parent': 0
        },
        'kind': '',
        'date_begin': '',
        'date_end': '',
        'cost': '',
        'share': '',
        'child_liquidated': ''
    }

    let result = getInputValue(data)
    if (!result){
        alert("Поля parent или child пустые или содержат не число");
        return;
    }

    fetch(`http://46.48.3.74:8000/` + "getdata", {
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
                parserDataPostgres(data)
            } else if (selectDB.options[selectDB.selectedIndex].value === "Neo4j") {
                parserDataNeo4j(data)
            } else if (selectDB.options[selectDB.selectedIndex].value === "OrientDB") {
                parserDataOrientDB(data)
            }

        });
}

function setPage(id) {
    let btns = document.querySelectorAll(".page-btn");
    btns.forEach(btn => {
        btn.style.display = "none";
    })
    document.getElementById(id).style.display = "block";
}

function parserDataOrientDB(data) {
    console.log(data)

    document.querySelector('.pages').innerHTML = "";
    document.querySelector('.data').innerHTML = "";
    let all_records = document.createElement("div");

    let timer = document.createElement("div");
    timer.classList.add("time");
    timer.innerHTML = `Найдено за ${data['time']} сек`;
    document.querySelector('.data').append(timer);

    let records = data['result']

    let page_count = Math.ceil(records.length / 25)
    for (let i = 1; i <= page_count; i++) {
        let page_btn = document.createElement("button");
        page_btn.addEventListener("click", () => {
            let blocks = document.querySelectorAll(".block-of-records");
            blocks.forEach(b => {
                b.style.display = "none";
            })
            document.getElementById(i.toString()).style.display = "block";
        })
        page_btn.setAttribute("class", `page-btn`);
        page_btn.innerHTML = `${i}`;
        document.querySelector(".pages").append(page_btn);

        let r = document.createElement("div");
        r.setAttribute("id", i.toString());
        r.setAttribute("class", "block-of-records");

        for (let j = (i - 1) * 25; j < 25 + (i - 1) * 25; j++) {
            if (j === records.length) {
                break;
            }
            let record_container = document.createElement("div");
            record_container.classList.add("record-container");

            let link_container = document.createElement("div");
            link_container.classList.add("link-container");

            let node_container = document.createElement("div");
            node_container.classList.add("nodes-container")

            node_container.innerHTML = `
                            <div class="node-item">face_id: ${records[j]['id']}</div>
                            <div class="node-item">face_name: ${records[j]['name']}</div>
                            <div class="node-item">face_type: ${records[j]['type']}</div>
                        `

            delete records[j].id;
            delete records[j].name;
            delete records[j].type;

            for (let e in records[j]) {
                let item = document.createElement("div")
                item.classList.add("recordItem");
                item.innerHTML = `${e}: ${records[j][e]}`
                link_container.append(item)
            }

            record_container.append(node_container);
            record_container.append(link_container);

            r.append(record_container);
        }
        if (i > 1) {
            r.style.display = "none"
        }
        all_records.append(r)
    }

    document.querySelector('.data').append(all_records);
    document.getElementById("spinner").remove()
}

function parserDataNeo4j(data) {
    document.querySelector('.data').innerHTML = "";
    document.querySelector('.pages').innerHTML = "";

    let time_block = document.createElement("div");
    time_block.classList.add("time");
    time_block.innerHTML = `Найдено за ${data['time']} сек`
    document.querySelector('.data').append(time_block)


    let page_count = Math.ceil(data['result'].length / 25)
    for (let i = 1; i <= page_count; i++) {
        let page_btn = document.createElement("button");
        page_btn.addEventListener("click", () => {
            let blocks = document.querySelectorAll(".block-of-records");
            blocks.forEach(b => {
                b.style.display = "none";
            })
            document.getElementById(i.toString()).style.display = "block";
        })
        page_btn.setAttribute("class", `page-btn`);
        page_btn.innerHTML = `${i}`;
        document.querySelector(".pages").append(page_btn);

        let records = document.createElement("div");
        records.setAttribute("id", i.toString());
        records.setAttribute("class", "block-of-records");

        for (let j = (i - 1) * 25; j < 25 + (i - 1) * 25; j++) {
            if (j === data.result.length) {
                break;
            }
            let record = data['result'][j]
            let node_type = ''

            let record_container = document.createElement("div");

            if (Object.keys(record).length !== 1) {
                node_type = Object.keys(record)[1]
                let parent_container = document.createElement("div");
                parent_container.classList.add("parentContainer")
                parent_container.innerHTML = `
                            <div class="parentItem pk">pk: ${record[node_type]['pk']}</div>
                            <div class="parentItem face-name">name: ${record[node_type]['face_name']}</div>
                            <div class="parentItem face-type">type: ${record[node_type]['face_type']}</div>
                        `
                record_container.append(parent_container);
            }

            let properties = document.createElement("div");
            properties.classList.add("propertiesContainer");
            properties.innerHTML = `
                        <div class="propertiesItem kind">тип связи: ${record['PROPERTIES(r)']['kind']}</div>
                        <div class="propertiesItem date-begin">начало: ${record['PROPERTIES(r)']['date_begin']}</div>
                        <div class="propertiesItem date-end">конец: ${record['PROPERTIES(r)']['date_end']}</div>
                        <div class="propertiesItem share">доля: ${record['PROPERTIES(r)']['share']}</div>
                        <div class="propertiesItem cost">стоимость: ${record['PROPERTIES(r)']['cost']}</div>
                        <div class="propertiesItem child-liquidated">child-liquidated: ${record['PROPERTIES(r)']['child_liquidated']}</div>

                    `
            record_container.append(properties);
            records.append(record_container);
            if (i > 1) {
                records.style.display = "none";
            }
        }
        document.querySelector('.data').append(records);

    }
    document.getElementById("spinner").remove();
}

function parserDataPostgres(data) {
    document.querySelector('.pages').innerHTML = "";
    document.querySelector('.data').innerHTML = "";
    let all_records = document.createElement("div");
    let temp = data["result"].split(']');
    let nodes = data["nodes"];
    let timer = document.createElement("div");
    timer.classList.add("time");
    timer.innerHTML = `Найдено за ${temp[1]} сек`;
    document.querySelector('.data').append(timer);

    let records = temp[0].split("{");
    records.splice(0, 1);

    let page_count = Math.ceil(records.length / 25)
    for (let i = 1; i <= page_count; i++) {
        let page_btn = document.createElement("button");
        page_btn.addEventListener("click", () => {
            let blocks = document.querySelectorAll(".block-of-records");
            blocks.forEach(b => {
                b.style.display = "none";
            })
            document.getElementById(i.toString()).style.display = "block";
        })
        page_btn.setAttribute("class", `page-btn`);
        page_btn.innerHTML = `${i}`;
        document.querySelector(".pages").append(page_btn);

        let r = document.createElement("div");
        r.setAttribute("id", i.toString());
        r.setAttribute("class", "block-of-records");

        for (let j = (i - 1) * 25; j < 25 + (i - 1) * 25; j++) {
            if (j === records.length) {
                break;
            }
            let record_container = document.createElement("div");
            record_container.classList.add("record-container");

            let link_container = document.createElement("div");
            link_container.classList.add("link-container");
            let record_arr = records[j].split(",");

            if (data["nodes_type"] !== '') {
                let node_container = document.createElement("div")
                node_container.classList.add("nodes-container")
                if (data["nodes_type"] === 'child') {
                    node_container.innerHTML = `
                                    <div class="node-title">Child</div>
                                `
                } else {
                    node_container.innerHTML = `
                                    <div class="node-title">Parent</div>
                                `
                }
                let node_field = nodes[j].substring(2, nodes[j].length - 2)
                node_field = node_field.split(",")
                for (let k = 0; k < node_field.length; k++) {
                    let node_item = document.createElement("div");
                    node_item.classList.add("node-item");
                    node_item.innerHTML = node_field[k];
                    node_container.append(node_item);
                }
                record_container.append(node_container);
            }

            for (let k = 1; k < record_arr.length; k++) {
                if (record_arr[k] !== " ") {
                    let block = document.createElement("div");
                    block.classList.add("recordItem");
                    block.innerHTML = record_arr[k];
                    link_container.append(block);
                }
            }
            record_container.append(link_container);

            r.append(record_container);
        }
        if (i > 1) {
            r.style.display = "none"
        }
        all_records.append(r)
    }

    document.querySelector('.data').append(all_records);
    document.getElementById("spinner").remove();
}