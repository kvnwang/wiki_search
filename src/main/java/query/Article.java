package query;

import java.io.Serializable;

public class Article implements Serializable {
	private String url;
	private String pos; 
	private String id;
	private String word;
	
	public Article(String id, String pos, String url, String word) {
		this.id=id;
		this.pos=pos;
		this.url=url;
		this.word=word;
	}

	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getPos() {
		return pos;
	}
	public void setPos() {
		this.pos=pos;
	}

	public String getWord() {
		return word;
	}
	public void setWord(String word) {
		this.word = word;
	}
	public String toString() {
		return "{"+id+pos+url+word+'}';
	}
	
	
}
