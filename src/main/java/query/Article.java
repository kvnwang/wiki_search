package query;

import java.io.Serializable;

public class Article implements Serializable {
	private String url;
	private String pos, pos1, pos2; 
	private String id;
	private String word1, word2, word;
	private String neighbor;


	public Article(String id, String pos1, String pos2, String url, String word1, String word2) {
		this.id=id;
		this.pos1=pos1;
		this.url=url;
		this.word1=word1;
		this.word2=word2;
	}
	public Article(String id, String pos, String url, String words) {
		this.id=id;
		this.pos=pos;
		this.url=url;
		this.word=words;
	}
	public Article(String word, String id, String pos, String url, String neighbor) {
		System.out.println(word);
		System.out.println(id);
		System.out.println(pos);
		System.out.println(url);
		System.out.println(neighbor);

		this.word=word;
		this.id=id;
		this.pos=pos;
		this.url=url;
		this.neighbor=neighbor;
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
		return "{"+id+pos1+pos2+url+word+neighbor+'}';
	}
	
	
}
