package com.qfree.obotest.entity;

import java.io.Serializable;
import javax.persistence.*;
import java.util.Set;


/**
 * The persistent class for the COUNTRY database table.
 * 
 */
@Entity
@NamedQuery(name="Country.findAll", query="SELECT c FROM Country c")
public class Country extends EntityBase implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	private Integer id;

	private String name;

	//bi-directional many-to-one association to City
	@OneToMany(mappedBy="country")
	private Set<City> cities;

	//bi-directional many-to-one association to Zip
	@OneToMany(mappedBy="country")
	private Set<Zip> zips;

	public Country() {
	}

	public Integer getId() {
		return this.id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Set<City> getCities() {
		return this.cities;
	}

	public void setCities(Set<City> cities) {
		this.cities = cities;
	}

	public City addCity(City city) {
		getCities().add(city);
		city.setCountry(this);

		return city;
	}

	public City removeCity(City city) {
		getCities().remove(city);
		city.setCountry(null);

		return city;
	}

	public Set<Zip> getZips() {
		return this.zips;
	}

	public void setZips(Set<Zip> zips) {
		this.zips = zips;
	}

	public Zip addZip(Zip zip) {
		getZips().add(zip);
		zip.setCountry(this);

		return zip;
	}

	public Zip removeZip(Zip zip) {
		getZips().remove(zip);
		zip.setCountry(null);

		return zip;
	}

}
