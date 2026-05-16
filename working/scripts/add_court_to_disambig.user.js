// <nowiki>
(function () {
	'use strict';

	var PAGE_BUTTON_TITLE = '加入又一法院';
	var PAGE_BUTTON_ID = 'add-another-court-to-disambig';
	var SECTION_LINK_CLASS = 'add-court-document-to-disambig-section';
	var DISAMBIG_CATEGORY = '裁判文书消歧义页';

	function getCurrentPageName() {
		return mw.config.get('wgRelevantPageName') || mw.config.get('wgPageName');
	}

	function hasDisambiguationCategory() {
		var categories = mw.config.get('wgCategories') || [];
		if (categories.indexOf(DISAMBIG_CATEGORY) !== -1) {
			return true;
		}

		return Array.prototype.some.call(
			document.querySelectorAll('#mw-normal-catlinks a, #mw-hidden-catlinks a'),
			function (link) {
				return link.textContent.trim() === DISAMBIG_CATEGORY;
			}
		);
	}

	function normalizePageName(input) {
		var pageName = input.trim();
		var linkMatch = pageName.match(/^\[\[\s*([^\]|#]+)(?:#[^\]|]*)?(?:\|[^\]]*)?\]\]$/);
		if (linkMatch) {
			pageName = linkMatch[1].trim();
		}

		return pageName.replace(/_/g, ' ');
	}

	function promptPageName(message) {
		var input = window.prompt(message);
		if (input === null) {
			return null;
		}

		var pageName = normalizePageName(input);
		return pageName || null;
	}

	function inferCourt(pageName) {
		var match = pageName.match(/^(.+?法院)[（(]/);
		return match ? match[1].trim() : null;
	}

	function buildCourtAppendText(pageName) {
		var court = inferCourt(pageName);
		if (!court) {
			throw new Error('无法从页面名推断法院：' + pageName);
		}

		return '\n' + [
			'==' + court + '==',
			'[[Category:' + court + ']]',
			'* [[' + pageName + ']]'
		].join('\n');
	}

	function buildDocumentAppendText(pageName) {
		return '\n* [[' + pageName + ']]';
	}

	function showError(error) {
		var message = error && error.message ? error.message : String(error);
		if (mw.notify) {
			mw.notify(message, { type: 'error' });
			return;
		}

		alert(message);
	}

	function reloadAfterSuccess() {
		window.setTimeout(function () {
			location.reload();
		}, 500);
	}

	function appendToPage(button) {
		var pageName = promptPageName('请输入法院案号页名：');
		if (!pageName) {
			return;
		}

		var appendText;
		try {
			appendText = buildCourtAppendText(pageName);
		} catch (error) {
			showError(error);
			return;
		}

		button.disabled = true;
		button.textContent = '正在加入...';

		new mw.Api().postWithToken('csrf', {
			action: 'edit',
			title: getCurrentPageName(),
			appendtext: appendText,
			summary: '',
			nocreate: 1,
			formatversion: 2
		}).then(reloadAfterSuccess).catch(function (error) {
			button.disabled = false;
			button.textContent = PAGE_BUTTON_TITLE;
			showError(error);
		});
	}

	function extractSectionNumber(href) {
		var url;
		try {
			url = new URL(href, location.href);
		} catch (error) {
			return null;
		}

		var section = url.searchParams.get('section');
		return /^\d+$/.test(section) ? section : null;
	}

	function appendToSection(link, sectionNumber) {
		if (link.dataset.busy === '1') {
			return;
		}

		var pageName = promptPageName('请输入案号页名：');
		if (!pageName) {
			return;
		}

		link.dataset.busy = '1';
		link.textContent = '正在加入...';

		new mw.Api().postWithToken('csrf', {
			action: 'edit',
			title: getCurrentPageName(),
			section: sectionNumber,
			appendtext: buildDocumentAppendText(pageName),
			summary: '',
			nocreate: 1,
			formatversion: 2
		}).then(reloadAfterSuccess).catch(function (error) {
			delete link.dataset.busy;
			link.textContent = '加入文書';
			showError(error);
		});
	}

	function addPageButton(parserOutput) {
		if (document.getElementById(PAGE_BUTTON_ID)) {
			return;
		}

		var button = document.createElement('button');
		button.id = PAGE_BUTTON_ID;
		button.type = 'button';
		button.className = 'cdx-button';
		button.title = PAGE_BUTTON_TITLE;
		button.textContent = PAGE_BUTTON_TITLE;
		button.addEventListener('click', function () {
			appendToPage(button);
		});

		parserOutput.appendChild(button);
	}

	function addSectionLinks() {
		Array.prototype.forEach.call(document.querySelectorAll('.mw-heading2'), function (heading) {
			var editSection = heading.querySelector('.mw-editsection');
			var target = heading.querySelector('.qe-target');
			var sectionNumber = target ? extractSectionNumber(target.href) : null;
			if (!editSection || !sectionNumber || heading.querySelector('.' + SECTION_LINK_CLASS)) {
				return;
			}

			var separator = document.createElement('span');
			separator.className = 'qe-section';
			separator.textContent = ' | ';

			var link = document.createElement('a');
			link.className = 'qe-section ' + SECTION_LINK_CLASS;
			link.href = '#';
			link.textContent = '加入文書';
			link.addEventListener('click', function (event) {
				event.preventDefault();
				appendToSection(link, sectionNumber);
			});

			var closingBracket = Array.prototype.find.call(
				editSection.querySelectorAll('.mw-editsection-bracket'),
				function (bracket) {
					return bracket.textContent.trim() === ']';
				}
			);

			if (closingBracket) {
				editSection.insertBefore(separator, closingBracket);
				editSection.insertBefore(link, closingBracket);
				return;
			}

			editSection.appendChild(separator);
			editSection.appendChild(link);
		});
	}

	function init() {
		var parserOutput = document.querySelector('.mw-parser-output');
		if (!parserOutput || !hasDisambiguationCategory()) {
			return;
		}

		addPageButton(parserOutput);
		addSectionLinks();
	}

	mw.loader.using('mediawiki.api').then(function () {
		if (document.readyState === 'loading') {
			document.addEventListener('DOMContentLoaded', init);
			return;
		}

		init();
	});
}());
// </nowiki>
