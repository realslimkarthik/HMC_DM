var TableData = React.createClass({
    render: function() {
        var inputData = this.props.items;
        var allcol = Object.keys(inputData[0]);

        return (<div>
                    <thead>
                        <tr>
                        {allcol.map(function(l){
                            var val = "search_" + l;
                            var name = "Search " + l;
                            return <th><label><input type="text" name={val} value={name} class="search_init" /></label></th>;
                        }) }
                        </tr>
                    </thead>
                    <tbody>
                        {inputData.map(function(l){
                            return <tr>
                                    {allcol.map(function(j){ 
                                        var val = eval("l." + j);
                                        return <td><input type="text" value={val}/></td>;
                                    })}
                                    </tr>;
                        }) }
                    </tbody></div>);
    }
});


