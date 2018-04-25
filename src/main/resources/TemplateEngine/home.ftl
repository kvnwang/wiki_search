<div class="starter-template">

<h2>${title}</h2>
 <p id="query"></p>
  <form action="" method="POST" role="form">
    <div class="form-group">
      <label for="query">Enter Query</label>
      <input type="text" class="form-control" id="query" name="query" placeholder="Enter query">
    </div>
    <button type="submit" class="b	tn btn-default">Submit</button>
  </form>
</div>
 <div id="body"></p>
<!-- Simple JS Function to convert the data into JSON and Pass it as ajax Call --!>
<script>
$(function() {
    $('form').submit(function(e) {
        e.preventDefault();
        var this_ = $(this);
        var array = this_.serializeArray();
        var json = {};
    
        $.each(array, function() {
            json[this.name] = this.value || '';
        });
        json = JSON.stringify(json);
    
        // Ajax Call
        $.ajax({
            type: "POST",
            url: "",
            data: json,
            success : function(response) {
                $("#status").text("Query added");
                 $("#body").html(response);
            },
            error : function(e) {
                $("#status").text("Query not added");
            }
        });
        $("html, body").animate({ scrollTop: 0 }, "slow");
        return false;
    });
});
