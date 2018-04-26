package models;
import wordwrapper.WikiWord;

import java.io.Serializable;

public class Article implements Serializable {
    private String url;
    private String pos;
    private String id;
    private String word;
    private String neighbor;
    private String title;

    public Article(String words, String id, String neighbor, String title) {
        this.word=words;
        this.id=id;
        this.pos=null;
        this.title=title;
        this.url="https://en.wikipedia.org/wiki?curid="+id;
        if(neighbor!=null) {
            this.neighbor=neighbor.trim();
        }
    }

    public String getword() {
        return word;
    }

    public String getPos() {
        return pos;
    }

    public String getUrl() {
        return url;
    }

    public String getId() {
        return id;
    }

    public String getTitle() {
        return this.title;
    }


    public String toString() {
        return this.word+ '\n'+title + '\n' + url + '\n' + neighbor+'\n';
    }
    @Override
    public boolean equals(Object o)  {
        if (o instanceof Article) {
            Article other = (Article) o;
            return ((Article) o).id.equals(id);
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return word.hashCode();
    }

}
