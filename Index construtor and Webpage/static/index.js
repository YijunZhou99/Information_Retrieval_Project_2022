// eel.expose(submit);
async function submit() {
    const query = document.getElementById("search-query").value;
    let resultContainer = document.getElementById("result-container");
    resultContainer.innerHTML = "";
    console.log(query)
    let raw_result = await eel.start_search(query)();

    let result = JSON.parse(raw_result[0])
    let resultLength = Object.keys(result["tf_idf_q"]).length;


    if (resultLength == 0) {
        let single_result = `
        <div class="row result-p">
        <div class="col-1">
        </div>
        <div class="col-10">
            <div class="alert alert-danger" role="alert">
                No results found, try searching with other queries
              </div>
        </div>
        <div class="col-1">
        </div>
      </div>
      `

      resultContainer.innerHTML += single_result;

    } else {

        let numAndTime = `
        <div class="alert alert-light" role="alert" style = "text-align:center;">
              Found ${raw_result[1]} results (${Math.round(raw_result[2]*1000)/1000} seconds)
        </div>
        `
        resultContainer.innerHTML += numAndTime;

        for (let i=0; i<resultLength; i++) {
            let url = "http://" + result["docurl"][i];
            let single_result = `
            <div class="row result-p">
            <div class="col-1">
            </div>
            <div class="col-10">
            <div class="card">
                <div class="card-body">
                    <h5 class="card-title">${result["title"][i]}</h5>
                    <h6 class="card-subtitle mb-2 text-muted">${result["docid_raw"][i]} </h6>
                    <p class="card-text">${result["excerpt"][i]}...</p>
                    <a href="${url}" class="card-link" target="_blank">${result["docurl"][i]}</a>
                    <p class="card-text"><strong>Cosine Similarity: </strong>${result["cosine"][i]}</p>
                </div>
                </div>
            </div>
            <div class="col-1">
            </div>
        </div>
        `

        resultContainer.innerHTML += single_result;
        }
    }
}