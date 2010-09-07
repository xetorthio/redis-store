# Redis Store

This is a Redis Store for tomcat sessions. It uses Jedis (http://github.com/xetorthio/jedis) as the Redis Client.

## How do I use it?

You can download the latests build at: 
    http://github.com/xetorthio/RedisStore/downloads

You must copy the jar into the lib folder of your tomcat installation directory, then edit your conf/server.xml file and within your context tag add the following lines:

	<Manager className="org.apache.catalina.session.PersistentManager" saveOnRestart="true" maxActiveSessions="1" minIdleSwap="1" maxIdleSwap="1" maxIdleBackup="1">
    	<Store className="org.apache.catalina.session.RedisStore" 
    		host="localhost"
    		port="6379"
    		password="something"
    		database="0"
    	/>
    </Manager>
	
You can play with the values. This is just an example.

License
-------

Copyright (c) 2010 Jonathan Leibiusky

Permission is hereby granted, free of charge, to any person
obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without
restriction, including without limitation the rights to use,
copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

