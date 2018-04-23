package query;

import java.io.Serializable;

public class Article implements Serializable {
	private String url;
	private String pos1, pos2; 
	private String id;
	private String word1, word2;
	


	public Article(String id, String pos1, String pos2, String url, String word1, String word2) {
		this.id=id;
		this.pos1=pos1;
		this.url=url;
		this.word1=word1;
		this.word2=word2;
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
	public String getPos1() {
		return pos1;
	}
	public String getPos2() {
		return pos2;
	}

	public String getWord2() {
		return word2;
	}
	public String getWord1() {
		return word1;
	}
	
	public String toString() {
		return "{"+id+pos1+pos2+url+word1+word2+'}';
	}
	
	
}
