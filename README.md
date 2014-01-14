Mapper Attachments Type for Elasticsearch
=========================================

The mapper attachments plugin adds the `attachment` type to Elasticsearch using [Apache Tika](http://lucene.apache.org/tika/).

In order to install the plugin, simply run: `bin/plugin -install elasticsearch/elasticsearch-mapper-attachments/1.9.0`.

|   Attachment Mapper Plugin  | elasticsearch         |  Tika  | Release date |
|-----------------------------|-----------------------|--------|:------------:|
| 2.0.0.RC1-SNAPSHOT (master) | 1.0.0.RC1 -> master   |  1.4   |              |
| 1.10.0-SNAPSHOT (1.x)       | 0.90.3 -> 0.90        |  1.4   |              |
| 1.9.0                       | 0.90.3 -> 0.90        |  1.4   |  2013-08-20  |
| 1.8.0                       | 0.90.3 -> 0.90        |  1.2   |  2013-08-07  |
| 1.7.0                       | 0.90.0 -> 0.90.2      |  1.2   |  2013-02-26  |
| 1.6.0                       | 0.19 -> 0.20          |  1.2   |  2012-09-28  |
| 1.5.0                       | 0.19 -> 0.20          |  1.2   |  2012-09-19  |
| 1.4.0                       | 0.19 -> 0.20          |  1.1   |  2012-03-25  |
| 1.3.0                       | 0.19 -> 0.20          |  1.0   |  2012-03-07  |
| 1.2.0                       | 0.19 -> 0.20          |  1.0   |  2012-02-15  |
| 1.1.0                       | 0.19 -> 0.20          |  1.0   |  2012-02-07  |
| 1.0.0                       | 0.18                  |  0.10  |  2011-12-05  |


The `attachment` type allows to index different "attachment" type field (encoded as `base64`), for example,
microsoft office formats, open document formats, ePub, HTML, and so on (full list can be found [here](http://tika.apache.org/1.4/formats.html)).

The `attachment` type is provided as a plugin extension. The plugin is a simple zip file that can be downloaded and placed under `$ES_HOME/plugins` location. It will be automatically detected and the `attachment` type will be added.

Using the attachment type is simple, in your mapping JSON, simply set a certain JSON element as attachment, for example:

```javascript
{
    "person" : {
        "properties" : {
            "my_attachment" : { "type" : "attachment" }
        }
    }
}
```

In this case, the JSON to index can be:

```javascript
{
    "my_attachment" : "... base64 encoded attachment ..."
}
```

Or it is possible to use more elaborated JSON if content type or resource name need to be set explicitly:

```javascript
{
    "my_attachment" : {
        "_content_type" : "application/pdf",
        "_name" : "resource/name/of/my.pdf",
        "content" : "... base64 encoded attachment ..."
    }
}
```

The `attachment` type not only indexes the content of the doc, but also automatically adds meta data on the attachment as well (when available).

The metadata supported are:

* `date`
* `title`
* `name` only available if you set `_name` see above
* `author`
* `keywords`
* `content_type`
* `content_length` is the original content_length before text extraction (aka file size)
* `language`

They can be queried using the "dot notation", for example: `my_attachment.author`.

Both the meta data and the actual content are simple core type mappers (string, date, ...), thus, they can be controlled in the mappings. For example:

```javascript
{
    "person" : {
        "properties" : {
            "file" : {
                "type" : "attachment",
                "fields" : {
                    "file" : {"index" : "no"},
                    "title" : {store : "yes"},
                    "date" : {"store" : "yes"},
                    "author" : {"analyzer" : "myAnalyzer"},
                    "keywords" : {store : "yes"},
                    "content_type" : {store : "yes"},
                    "content_length" : {store : "yes"},
                    "language" : {store : "yes"}
                }
            }
        }
    }
}
```

In the above example, the actual content indexed is mapped under `fields` name `file`, and we decide not to index it, so it will only be available in the `_all` field. The other fields map to their respective metadata names, but there is no need to specify the `type` (like `string` or `date`) since it is already known.

Indexed Characters
------------------

By default, `100000` characters are extracted when indexing the content. This default value can be changed by setting the `index.mapping.attachment.indexed_chars` setting. It can also be provided on a per document indexed using the `_indexed_chars` parameter. `-1` can be set to extract all text, but note that all the text needs to be allowed to be represented in memory.

Note, this feature is supported since `1.3.0` version.

Metadata parsing error handling
-------------------------------

While extracting metadata content, errors could happen for example when parsing dates.
Since version `1.9.0`, parsing errors are ignored so your document is indexed.

You can disable this feature by setting the `index.mapping.attachment.ignore_errors` setting to `false`.

Language Detection
------------------

By default, language detection is disabled (`false`) as it could come with a cost.
This default value can be changed by setting the `index.mapping.attachment.detect_language` setting.
It can also be provided on a per document indexed using the `_detect_language` parameter.

Note, this feature is supported since `2.0.0.RC1` version.


License
-------

    This software is licensed under the Apache 2 license, quoted below.

    Copyright 2009-2014 Elasticsearch <http://www.elasticsearch.org>

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
