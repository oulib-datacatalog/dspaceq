<?xml version="1.0" encoding="UTF-8"?>
<!-- Derived from https://gist.github.com/kardeiz/4504802 -->
<!-- Derived from imlsdcc.grainger.uiuc.edu/docs/stylesheets/GeneralMARCtoQDC.xsl -->
<xsl:stylesheet version="1.0"
                xmlns:marc="http://www.loc.gov/MARC21/slim"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:dcterms="http://purl.org/dc/terms/"
                exclude-result-prefixes="marc">
  <xsl:template name="subfieldSelect">
    <xsl:param name="codes"/>
    <xsl:param name="delimeter">
      <xsl:text> </xsl:text>
    </xsl:param>
    <xsl:variable name="str">
      <xsl:for-each select="marc:subfield">
        <xsl:if test="contains($codes, @code)">
          <xsl:value-of select="text()"/>
          <xsl:value-of select="$delimeter"/>
        </xsl:if>
      </xsl:for-each>
    </xsl:variable>
    <xsl:value-of select="substring($str,1,string-length($str)-string-length($delimeter))"/>
  </xsl:template>
  <xsl:template name="noteUriRelation">
    <xsl:param name="noteSubfields">nt</xsl:param>
    <xsl:param name="uriSubfields">o</xsl:param>
    <xsl:param name="dcValue">dcvalue</xsl:param>
    <xsl:param name="elementName">relation</xsl:param>
    <xsl:param name="qualifierName">isversionof</xsl:param>
    <xsl:variable name="note">
      <xsl:call-template name="subfieldSelect">
        <xsl:with-param name="codes">
          <xsl:value-of select="$noteSubfields"/>
        </xsl:with-param>
      </xsl:call-template>
    </xsl:variable>
    <xsl:variable name="url">
      <xsl:call-template name="subfieldSelect">
        <xsl:with-param name="codes">
          <xsl:value-of select="$uriSubfields"/>
        </xsl:with-param>
      </xsl:call-template>
    </xsl:variable>
    <xsl:if test="string-length($note) &gt; 0">
      <xsl:element name="{$dcValue}">
        <xsl:attribute name="element">
          <xsl:value-of select="$elementName"/>
        </xsl:attribute>
        <xsl:attribute name="qualifier">
          <xsl:value-of select="$qualifierName"/>
        </xsl:attribute>
        <xsl:value-of select="$note"/>
      </xsl:element>
    </xsl:if>
    <xsl:if test="string-length($url) &gt; 0">
      <xsl:element name="{$dcValue}">
        <xsl:attribute name="element">
          <xsl:value-of select="$elementName"/>
        </xsl:attribute>
        <xsl:attribute name="qualifier">
          <xsl:value-of select="'uri'"/>
        </xsl:attribute>
        <xsl:value-of select="$url"/>
      </xsl:element>
    </xsl:if>
  </xsl:template>

  <xsl:output method="xml" indent="yes"/>
  <xsl:template match="/">
    <xsl:if test="marc:collection">
      <collection>
        <xsl:for-each select="marc:collection">
          <xsl:for-each select="marc:record">
            <dublin_core>
              <xsl:apply-templates select="."/>
            </dublin_core>
          </xsl:for-each>
        </xsl:for-each>
      </collection>
    </xsl:if>
    <xsl:if test="marc:record">
      <dublin_core>
        <xsl:apply-templates/>
      </dublin_core>
    </xsl:if>
  </xsl:template>

  <xsl:template match="marc:record">
    <xsl:variable name="leader" select="marc:leader"/>
    <xsl:variable name="leader6" select="substring($leader,7,1)"/>
    <xsl:variable name="leader7" select="substring($leader,8,1)"/>
    <xsl:variable name="controlField008" select="marc:controlfield[@tag=008]"/>
    <xsl:for-each select="marc:datafield[@tag=245]">
      <dcvalue element="title" qualifier="none">
        <xsl:call-template name="subfieldSelect">
          <xsl:with-param name="codes">abfghknp</xsl:with-param>
        </xsl:call-template>
      </dcvalue>
    </xsl:for-each>
    <!-- find alternative representations of the title -->
    <xsl:for-each select="marc:datafield[@tag=130]|marc:datafield[@tag=210]|marc:datafield[@tag=240]|marc:datafield[@tag=242]|marc:datafield[@tag=246]|marc:datafield[@tag=730]|marc:datafield[@tag=740]">
      <dcvalue element="title" qualifier="alternative">
        <xsl:call-template name="subfieldSelect">
          <xsl:with-param name="codes">
            <xsl:choose>
              <xsl:when test="@tag=130 or @tag=240 or @tag=246">adfghklmnoprst</xsl:when>
              <xsl:when test="@tag=210">ab</xsl:when>
              <xsl:when test="@tag=242 or @tag=730">abcdfghklmnoprst</xsl:when>
              <xsl:when test="@tag=740">ahnp</xsl:when>
            </xsl:choose>
          </xsl:with-param>
        </xsl:call-template>
      </dcvalue>
    </xsl:for-each>
    <!-- generate creator tags; see http://www.loc.gov/marc/marc2dc.html for
         information on why these are creator tags and not contributor tags -->
    <xsl:for-each select="marc:datafield[@tag=100]|marc:datafield[@tag=110]|marc:datafield[@tag=111]|marc:datafield[@tag=700]|marc:datafield[@tag=710]|marc:datafield[@tag=711]|marc:datafield[@tag=720]">
      <dcvalue element="contributor" qualifier="author">
        <xsl:value-of select="."/>
      </dcvalue>
    </xsl:for-each>
    <!-- generate subject tags -->
    <!-- using 655 here overlaps with handling
         of 655 later in the stylesheet -->
    <xsl:for-each select="marc:datafield[@tag=600]|marc:datafield[@tag=610]|marc:datafield[@tag=611]|marc:datafield[@tag=630]|marc:datafield[@tag=650]|marc:datafield[@tag=651]|marc:datafield[@tag=653]|marc:datafield[@tag=654]|marc:datafield[@tag=655]|marc:datafield[@tag=656]|marc:datafield[@tag=657]|marc:datafield[@tag=658]">
      <dcvalue element="subject">
        <!-- the <xsl:attribute> tag goes inside the when clause because in
             some cases we will not add the type attribute -->
        <xsl:choose>
          <xsl:when test="@ind2=0">
            <xsl:attribute name="qualifier">lcsh</xsl:attribute>
          </xsl:when>
          <xsl:when test="@ind2=2">
            <xsl:attribute name="qualifier">mesh</xsl:attribute>
          </xsl:when>
          <xsl:otherwise>
            <xsl:attribute name="qualifier">none</xsl:attribute>
          </xsl:otherwise>
        </xsl:choose>
        <!-- now populate the value of the tag -->
        <xsl:call-template name="subfieldSelect">
          <xsl:with-param name="codes">abcdqvxyz</xsl:with-param>
        </xsl:call-template>
      </dcvalue>
    </xsl:for-each>
    <!-- generate subject tag for Library of Congress catalog number -->
    <xsl:for-each select="marc:datafield[@tag=050]">
      <dcvalue element="subject" qualifier="lcc">
        <xsl:value-of select="."/>
      </dcvalue>
    </xsl:for-each>
    <!-- generate subject tag for Dewey Decimal catalog number -->
    <xsl:for-each select="marc:datafield[@tag=082]">
      <dcvalue element="subject" qualifier="ddc">
        <xsl:value-of select="."/>
      </dcvalue>
    </xsl:for-each>
    <!-- generate subject tag for Universal Decimal catalog number -->
    <xsl:for-each select="marc:datafield[@tag=080]">
      <dcvalue element="subject" qualifier="none">
        <xsl:value-of select="."/>
      </dcvalue>
    </xsl:for-each>
    <!-- the 500 range tags are mostly dc:description tags in the output
         with some exceptions -->
    <xsl:for-each select="marc:datafield[@tag &gt;=500 and @tag &lt;= 599]">
      <xsl:choose>
        <!-- the 505 tag is a table of contents entry -->
        <xsl:when test="@tag=505">
          <dcvalue element="description" qualifier="tableofcontents">
            <xsl:value-of select="."/>
          </dcvalue>
        </xsl:when>
        <!-- 506 specifies the rights to a particular item -->
        <!-- 540 specifies the rights to a particular item -->
        <xsl:when test="@tag=506 or @tag=540">
          <xsl:call-template name="noteUriRelation">
            <xsl:with-param name="noteSubfields">abcd3</xsl:with-param>
            <xsl:with-param name="uriSubfields">u</xsl:with-param>
            <xsl:with-param name="elementName">rights</xsl:with-param>
            <xsl:with-param name="qualifierName">none</xsl:with-param>
          </xsl:call-template>
        </xsl:when>
        <!-- 510 is a citation reference note -->
        <xsl:when test="@tag=510">
          <dcvalue element="relation" qualifier="isreferencedby">
            <xsl:value-of select="."/>
          </dcvalue>
        </xsl:when>
        <!-- 513$b contains period covered note -->
        <xsl:when test="@tag=513">
          <dcvalue element="coverage" qualifier="temporal">
            <xsl:value-of select="."/>
          </dcvalue>
        </xsl:when>
        <!-- the 520 field is an abstract if indicator 1 is set to 3 -->
        <xsl:when test="@tag=520 and @ind1=3">
          <dcvalue element="description" qualifier="abstract">
            <xsl:value-of select="."/>
          </dcvalue>
        </xsl:when>
        <!-- 522$a is spatial coverage -->
        <xsl:when test="@tag=522">
          <dcvalue element="coverage" qualifier="spatial">
            <xsl:value-of select="."/>
          </dcvalue>
        </xsl:when>
        <!-- 530 additional physical format available -->
        <xsl:when test="@tag=530">
          <xsl:call-template name="noteUriRelation">
            <xsl:with-param name="noteSubfields">abcd3</xsl:with-param>
            <xsl:with-param name="uriSubfields">u</xsl:with-param>
            <xsl:with-param name="elementName">relation</xsl:with-param>
            <xsl:with-param name="elementName">isformatof</xsl:with-param>
          </xsl:call-template>
          <!--
	  <xsl:call-template name="noteUriRelation">
	    <xsl:with-param name="noteSubfields">abcd3</xsl:with-param>
	    <xsl:with-param name="uriSubfields">u</xsl:with-param>
	    <xsl:with-param name="elementName">dcterms:hasFormat</xsl:with-param>
	  </xsl:call-template>
	  -->
        </xsl:when>
        <!-- 538 specifies this item requires another -->
        <xsl:when test="@tag=538">
          <xsl:call-template name="noteUriRelation">
            <xsl:with-param name="noteSubfields">ai3</xsl:with-param>
            <xsl:with-param name="uriSubfields">u</xsl:with-param>
            <xsl:with-param name="elementName">relation</xsl:with-param>
            <xsl:with-param name="qualifierName">requires</xsl:with-param>
          </xsl:call-template>
        </xsl:when>
        <!-- http://www.loc.gov/marc/marc2dc.html claims datafield contains
	     RFC1766 language codes. That doesn't appear to genereally be
	     the case. -->
        <xsl:when test="@tag=546">
          <dcvalue element="language" qualifier="none">
            <xsl:call-template name="subfieldSelect">
              <xsl:with-param name="codes">ab3</xsl:with-param>
            </xsl:call-template>
          </dcvalue>
        </xsl:when>
        <!-- 561 Custodial History -->
        <xsl:when test="@tag=561">
          <dcvalue element="description" qualifier="provenance">
            <xsl:value-of select="."/>
          </dcvalue>
        </xsl:when>
        <!-- everything else is just a description -->
        <xsl:otherwise>
          <dcvalue element="description" qualifier="none">
            <xsl:value-of select="."/>
          </dcvalue>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:for-each>
    <!-- extract the publisher name and creation date -->
    <xsl:for-each select="marc:datafield[@tag=260]">
      <!-- publisher tag is contained in 260$a$b -->
      <xsl:variable name="publisher">
        <xsl:call-template name="subfieldSelect">
          <xsl:with-param name="codes">ab</xsl:with-param>
        </xsl:call-template>
      </xsl:variable>
      <xsl:if test="string-length($publisher) &gt; 0">
        <dcvalue element="publisher" qualifier="none">
          <xsl:value-of select="$publisher"/>
        </dcvalue>
      </xsl:if>
      <!-- created tag is contained in 260$c$g -->
      <xsl:variable name="created">
        <xsl:call-template name="subfieldSelect">
          <xsl:with-param name="codes">cg</xsl:with-param>
        </xsl:call-template>
      </xsl:variable>
      <xsl:if test="string-length($created) &gt; 0">
        <dcvalue element="date" qualifier="created">
          <xsl:value-of select="$created"/>
        </dcvalue>
      </xsl:if>
      <!-- issued tag is contained in 260$c -->
      <xsl:for-each select="marc:subfield[@code='c']">
        <dcvalue element="date" qualifier="issued">
          <xsl:value-of select="."/>
        </dcvalue>
      </xsl:for-each>
    </xsl:for-each>
    <!-- 533$d contains data for a created tag -->
    <xsl:for-each select="marc:datafield[@tag=533]/marc:subfield[@code='d']">
      <dcvalue element="date" qualifier="created">
        <xsl:value-of select="."/>
      </dcvalue>
    </xsl:for-each>
    <!-- chars 7 through 10 of controlfield 008 contain a created date -->
    <xsl:variable name="controlFieldDate">
      <xsl:value-of select="normalize-space(substring($controlField008,8,4))"/>
    </xsl:variable>
    <xsl:if test="string-length($controlFieldDate) &gt; 0">
      <dcvalue element="date" qualifier="created">
        <xsl:value-of select="$controlFieldDate"/>
      </dcvalue>
    </xsl:if>
    <!-- generate type information based on leader6 and leader 7 data -->
    <xsl:if test="string-length(normalize-space($leader6))">
      <xsl:choose>
        <xsl:when test="contains('acdt', $leader6)">
          <dcvalue element="type" qualifier="none">Text</dcvalue>
        </xsl:when>
        <xsl:when test="contains('efgk', $leader6)">
          <dcvalue element="type" qualifier="none">Image</dcvalue>
        </xsl:when>
        <xsl:when test="contains('ij', $leader6)">
          <dcvalue element="type" qualifier="none">Sound</dcvalue>
        </xsl:when>
        <xsl:when test="$leader6='m'">
          <dcvalue element="type" qualifier="none">Software</dcvalue>
        </xsl:when>
        <xsl:when test="contains('opr', $leader6)">
          <dcvalue element="type" qualifier="none">PhysicalObject</dcvalue>
        </xsl:when>
      </xsl:choose>
      <!-- refined types -->
      <xsl:choose>
        <xsl:when test="contains('efk', $leader6)">
          <dcvalue element="type" qualifier="none">StillImage</dcvalue>
        </xsl:when>
        <xsl:when test="$leader6='g'">
          <dcvalue element="type" qualifier="none">MovingImage</dcvalue>
        </xsl:when>
        <xsl:when test="contains('cd', $leader6)">
          <dcvalue element="type" qualifier="none">NotatedMusic</dcvalue>
        </xsl:when>
        <xsl:when test="$leader6='p'">
          <dcvalue element="type" qualifier="none">MixedMaterial</dcvalue>
        </xsl:when>
      </xsl:choose>
      <!-- even more refinement on information in leader6 -->
      <xsl:if test="contains('ef', $leader6)">
        <dcvalue element="type" qualifier="none">Cartographic</dcvalue>
      </xsl:if>
    </xsl:if>
    <!-- end checks on $leader6 -->
    <!-- leader 7 can tell us if something is a collection -->
    <xsl:if test="string-length(normalize-space($leader7))">
      <xsl:choose>
        <xsl:when test="contains('csp', $leader7)">
          <dcvalue element="type" qualifier="none">Collection</dcvalue>
        </xsl:when>
      </xsl:choose>
    </xsl:if>
    <!-- the 655 field in marc might contain DC type information -->
    <xsl:for-each select="marc:datafield[@tag=655]">
      <dcvalue element="type" qualifier="none">
        <xsl:call-template name="subfieldSelect">
          <xsl:with-param name="codes">abcvxyz3568</xsl:with-param>
        </xsl:call-template>
      </dcvalue>
    </xsl:for-each>
    <!-- get the format code -->
    <xsl:for-each select="marc:datafield[@tag=856]/marc:subfield[@code='q']">
      <dcvalue element="format" qualifier="mimetype">
        <xsl:value-of select="."/>
      </dcvalue>
    </xsl:for-each>
    <!-- generate format extent tags -->
    <xsl:for-each select="marc:datafield[@tag=300]/marc:subfield[@code='a']">
      <dcvalue element="format" qualifier="extent">
        <xsl:value-of select="."/>
      </dcvalue>
    </xsl:for-each>
    <!-- 533$e contains data for an extent tag -->
    <xsl:for-each select="marc:datafield[@tag=533]/marc:subfield[@code='e']">
      <dcterms:extent>
        <xsl:value-of select="."/>
      </dcterms:extent>
    </xsl:for-each>
    <!-- 300 contains data about the medium -->
    <xsl:for-each select="marc:datafield[@tag=300]">
      <dcvalue element="format" qualifier="medium">
        <xsl:value-of select="."/>
      </dcvalue>
    </xsl:for-each>
    <!-- 340$a contains data about the medium -->
    <xsl:for-each select="marc:datafield[@tag=340]/marc:subfield[@code='a']">
      <dcvalue element="format" qualifier="medium">
        <xsl:value-of select="."/>
      </dcvalue>
    </xsl:for-each>
    <!-- 856$u contains the URI for the item -->
    <xsl:for-each select="marc:datafield[@tag=856]/marc:subfield[@code='u']">
      <dcvalue element="identifier" qualifier="uri">
        <xsl:value-of select="."/>
      </dcvalue>
    </xsl:for-each>
    <!-- controlfield 8 chars 35 through 37 contain an ISO 639-2 language
         code -->
    <xsl:variable name="controlFieldLang">
      <xsl:value-of select="normalize-space(substring($controlField008,36,3))"/>
    </xsl:variable>
    <xsl:if test="string-length($controlFieldLang) &gt; 0">
      <dcvalue element="language" qualifier="iso">
        <xsl:value-of select="$controlFieldLang"/>
      </dcvalue>
    </xsl:if>
    <!-- 041 fields contain lists of ISO639-2 language codes -->
    <xsl:for-each select="marc:datafield[@tag=041]">
      <xsl:for-each select="marc:subfield[@code='a']|marc:subfield[@code='b']|marc:subfield[@code='d']|marc:subfield[@code='e']|marc:subfield[@code='f']|marc:subfield[@code='g']|marc:subfield[@code='h']">
        <dcvalue element="language" qualifier="iso">
          <xsl:value-of select="."/>
        </dcvalue>
      </xsl:for-each>
    </xsl:for-each>
    <!-- 775 contains isVersionOf and hasVersion information -->
    <xsl:for-each select="marc:datafield[@tag=775]">
      <xsl:call-template name="noteUriRelation"/>
      <xsl:call-template name="noteUriRelation">
        <xsl:with-param name="elementName">relation</xsl:with-param>
        <xsl:with-param name="qualifierName">hasversion</xsl:with-param>
      </xsl:call-template>
    </xsl:for-each>
    <!-- 786 contains isVersionOf and source information -->
    <xsl:for-each select="marc:datafield[@tag=786]">
      <xsl:call-template name="noteUriRelation"/>
      <xsl:call-template name="noteUriRelation">
        <xsl:with-param name="elementName">source</xsl:with-param>
        <xsl:with-param name="qualifierName">none</xsl:with-param>
      </xsl:call-template>
    </xsl:for-each>
    <!-- 785 specifies that this item is replaced by another item -->
    <xsl:for-each select="marc:datafield[@tag=785]">
      <xsl:call-template name="noteUriRelation">
        <xsl:with-param name="elementName">relation</xsl:with-param>
        <xsl:with-param name="qualifierName">isreplacedby</xsl:with-param>
      </xsl:call-template>
    </xsl:for-each>
    <!-- 780 specifies that this item replaces another item -->
    <xsl:for-each select="marc:datafield[@tag=780]">
      <xsl:call-template name="noteUriRelation">
        <xsl:with-param name="elementName">relation</xsl:with-param>
        <xsl:with-param name="qualifierName">replaces</xsl:with-param>
      </xsl:call-template>
    </xsl:for-each>
    <!-- 760 and 773 specify that this item is part of another -->
    <xsl:for-each select="marc:datafield[@tag=760]|marc:datafield[@tag=773]">
      <xsl:call-template name="noteUriRelation">
        <xsl:with-param name="elementName">relation</xsl:with-param>
        <xsl:with-param name="qualifierName">ispartof</xsl:with-param>
      </xsl:call-template>
    </xsl:for-each>
    <!-- it is claimed that tags 440, 490, 800, 810, 811, and 830 contain
	 "is part of" information; note, however, these relations do not
	 include a URI -->
    <xsl:for-each select="marc:datafield[@tag=440]|marc:datafield[@tag=490]|marc:datafield[@tag=800]|marc:datafield[@tag=810]|marc:datafield[@tag=811]|marc:datafield[@tag=830]">
      <dcvalue element="relation" qualifier="ispartof">
        <xsl:value-of select="."/>
      </dcvalue>
    </xsl:for-each>
    <!-- 774 indicates that another item has part of this item -->
    <xsl:for-each select="marc:datafield[@tag=774]">
      <xsl:call-template name="noteUriRelation">
        <xsl:with-param name="elementName">relation</xsl:with-param>
        <xsl:with-param name="qualifierName">haspart</xsl:with-param>
      </xsl:call-template>
    </xsl:for-each>
    <!-- 776 indicates this item is available in another format -->
    <xsl:for-each select="marc:datafield[@tag=776]">
      <xsl:call-template name="noteUriRelation">
        <xsl:with-param name="elementName">relation</xsl:with-param>
        <xsl:with-param name="qualifierName">isformatof</xsl:with-param>
      </xsl:call-template>
      <xsl:call-template name="noteUriRelation">
       <xsl:with-param name="elementName">relation</xsl:with-param>
        <xsl:with-param name="qualifierName">hasformat</xsl:with-param>
      </xsl:call-template>
    </xsl:for-each>
    <!-- 255 contains cartographic mathematical data -->
    <!-- 650$z contains geographic subject entry -->
    <xsl:for-each select="marc:datafield[@tag=255]|marc:datafield[@tag=650]/marc:subfield[@code='z']">
      <dcvalue element="coverage" qualifier="spatial">
        <xsl:value-of select="."/>
      </dcvalue>
    </xsl:for-each>
    <!-- 651 is subject geographic name -->
    <xsl:for-each select="marc:datafield[@tag=651]">
      <dcvalue element="coverage" qualifier="spatial">
        <xsl:call-template name="subfieldSelect">
          <xsl:with-param name="codes">az</xsl:with-param>
        </xsl:call-template>
      </dcvalue>
    </xsl:for-each>
    <!-- 752 hierarchical place name -->
    <xsl:for-each select="marc:datafield[@tag=752]">
      <dcvalue element="coverage" qualifier="spatial">
        <xsl:call-template name="subfieldSelect">
          <xsl:with-param name="codes">abcd</xsl:with-param>
        </xsl:call-template>
      </dcvalue>
    </xsl:for-each>
    <!-- 043$c contains geographic area code -->
    <!-- 044$c contains contains country of publishing code -->
    <xsl:for-each select="marc:datafield[@tag=043]/marc:subfield[@code='c']|marc:datafield[@tag=044]/marc:subfield[@code='c']">
      <dcvalue element="coverage" qualifier="spatial">
        <xsl:value-of select="."/>
      </dcvalue>
    </xsl:for-each>
    <!-- 033$a contains date/time of event -->
    <xsl:for-each select="marc:datafield[@tag=033]/marc:subfield[@code='a']">
      <dcvalue element="coverage" qualifier="temporal">
        <xsl:value-of select="."/>
      </dcvalue>
    </xsl:for-each>
    <!-- the marc2dc crosswalk specifies that 760-787$o$t be translated to
         the relation tag; some, but not all, of the datafields in this
	 range are mentioned in the marc2qdc crosswalk. Those that have not
	 are handled here -->
    <xsl:for-each select="marc:datafield[(@tag &gt;= 761 and @tag &lt;= 772) or (@tag &gt;= 777 and @tag &lt;= 779) or (@tag &gt;= 781 and @tag &lt;= 784) or (@tag = 787)]">
      <dcvalue element="relation" qualifier="none">
        <xsl:call-template name="subfieldSelect">
          <xsl:with-param name="codes">ot</xsl:with-param>
        </xsl:call-template>
      </dcvalue>
    </xsl:for-each>
    <!-- 020 is mentioned in neither the marc2dc nor marc2qdc crosswalk;
         however, it contains ISBN information that can be listed as an
	 identifier field -->
    <xsl:for-each select="marc:datafield[@tag=020]/marc:subfield[@code='a']|marc:datafield[@tag=020]/marc:subfield[@code='z']">
      <dcvalue element="identifier" qualifier="uri">
        <xsl:text>URN:ISBN:</xsl:text>
        <xsl:value-of select="."/>
      </dcvalue>
    </xsl:for-each>
    <!-- the marc2dc stylesheet from which this stylesheet was derived
         includes this rule to handle the obsolete 090 datafield -->
    <xsl:for-each select="marc:datafield[@tag=090]">
      <dcvalue element="identifier" qualifier="none">
        <xsl:value-of select="."/>
      </dcvalue>
    </xsl:for-each>
  </xsl:template>
</xsl:stylesheet>