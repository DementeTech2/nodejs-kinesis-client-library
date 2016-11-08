"use strict";
var http_1 = require('http');
var responseHeaders = { 'Content-Type': 'text/plain' };
exports.create = function (port, callback) {
    var server = http_1.createServer(function (req, res) {
        try {
            var response = callback();
            res.writeHead(200, responseHeaders);
            res.end(response.toString());
        }
        catch (e) {
            res.writeHead(500, responseHeaders);
            res.end(e.toString());
        }
    });
    server.listen(port);
};
