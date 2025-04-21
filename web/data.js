function httpGet(theUrl) {
    var xmlHttp = new XMLHttpRequest();
    xmlHttp.open("GET", theUrl, false); // false for synchronous request
    xmlHttp.send(null);
    return xmlHttp.responseText;
}


function get_nodes() {
    const rawnodes = JSON.parse(httpGet("http://localhost:8000/all_nodes"));
    const nodes = [];
    for (let i = 0; i < rawnodes.length; i++) {
        nodes.push({
            id: rawnodes[i],
        })
    }
    return nodes;
}

function get_links() {
    const rawlinks = JSON.parse(httpGet("http://localhost:8000/all_links"));
    return rawlinks;


}