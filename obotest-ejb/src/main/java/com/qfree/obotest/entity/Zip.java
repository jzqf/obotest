package com.qfree.obotest.entity;

import java.io.Serializable;
import javax.persistence.*;


/**
 * The persistent class for the ZIP database table.
 * 
 */
@Entity
@NamedQuery(name="Zip.findAll", query="SELECT z FROM Zip z")
public class Zip extends EntityBase implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	private Integer id;

	private String code;

	private String name;

	//bi-directional many-to-one association to Country
	@ManyToOne
	private Country country;

	public Zip() {
	}

	public Integer getId() {
		return this.id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getCode() {
		return this.code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Country getCountry() {
		return this.country;
	}

	public void setCountry(Country country) {
		this.country = country;
	}

}