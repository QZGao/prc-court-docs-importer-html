// <nowiki>
(function () {
	'use strict';

	var BUTTON_TITLE = '轉換為裁判文書消歧義頁';
	var BUTTON_ID = 'convert-redirect-to-court-disambig';

	function isRedirectPage() {
		return mw.config.get('wgIsRedirect') === true ||
			document.querySelector('.mw-parser-output .redirectMsg') !== null;
	}

	function getCurrentPageName() {
		return mw.config.get('wgRelevantPageName') || mw.config.get('wgPageName');
	}

	function getCurrentTemplateTitle() {
		var title = mw.config.get('wgTitle') || getCurrentPageName();
		return title.replace(/_/g, ' ');
	}

	function extractRedirectTarget(wikitext) {
		var match = wikitext.match(/^\s*#\s*(?:REDIRECT|重定向|重新導向|重新导向)\s*:?\s*\[\[\s*([^\]|#]+)(?:#[^\]|]*)?(?:\|[^\]]*)?\]\]/im);
		return match ? match[1].trim().replace(/_/g, ' ') : null;
	}

	function inferCourt(targetTitle) {
		var match = targetTitle.match(/^(.+?法院)[（(]/);
		return match ? match[1].trim() : null;
	}

	function inferDocumentType(targetTitle) {
		var markerIndex = targetTitle.lastIndexOf('号');
		if (markerIndex === -1) {
			return null;
		}

		var documentType = targetTitle.slice(markerIndex + 1).trim();
		return documentType || null;
	}

	function buildDisambiguationText(pageTitle, targetTitle) {
		var court = inferCourt(targetTitle);
		var documentType = inferDocumentType(targetTitle);

		if (!court || !documentType) {
			throw new Error('无法从重定向目标推断法院或文书类型：' + targetTitle);
		}

		return [
			'{{裁判文书消歧义页',
			'|title=' + pageTitle,
			'|type=' + documentType,
			'}}',
			'==' + court + '==',
			'[[Category:' + court + ']]',
			'* [[' + targetTitle + ']]',
			''
		].join('\n');
	}

	function getRevisionContent(revision) {
		if (revision.slots && revision.slots.main) {
			return revision.slots.main.content || revision.slots.main['*'] || '';
		}

		return revision.content || revision['*'] || '';
	}

	function setButtonBusy(button, busy) {
		button.disabled = busy;
		button.textContent = busy ? '正在轉換...' : BUTTON_TITLE;
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

	function convertPage(button) {
		var api = new mw.Api();
		var pageName = getCurrentPageName();

		setButtonBusy(button, true);

		api.get({
			action: 'query',
			prop: 'revisions',
			titles: pageName,
			rvprop: 'ids|content',
			rvslots: 'main',
			formatversion: 2
		}).then(function (data) {
			var page = data.query.pages[0];
			var revision = page.revisions && page.revisions[0];
			var wikitext = revision ? getRevisionContent(revision) : '';
			var targetTitle = extractRedirectTarget(wikitext);

			if (!targetTitle) {
				throw new Error('当前页面的源码不是可识别的重定向语法。');
			}

			return api.postWithToken('csrf', {
				action: 'edit',
				title: pageName,
				text: buildDisambiguationText(getCurrentTemplateTitle(), targetTitle),
				summary: '',
				baserevid: revision.revid,
				nocreate: 1,
				formatversion: 2
			});
		}).then(function () {
			reloadAfterSuccess();
		}).catch(function (error) {
			setButtonBusy(button, false);
			showError(error);
		});
	}

	function addButton() {
		var parserOutput = document.querySelector('.mw-parser-output');
		if (!isRedirectPage() || !parserOutput || document.getElementById(BUTTON_ID)) {
			return;
		}

		var button = document.createElement('button');
		button.id = BUTTON_ID;
		button.type = 'button';
		button.className = 'cdx-button';
		button.title = BUTTON_TITLE;
		button.textContent = BUTTON_TITLE;
		button.addEventListener('click', function () {
			convertPage(button);
		});

		parserOutput.insertBefore(button, parserOutput.firstChild);
	}

	mw.loader.using('mediawiki.api').then(function () {
		if (document.readyState === 'loading') {
			document.addEventListener('DOMContentLoaded', addButton);
			return;
		}

		addButton();
	});
}());
// </nowiki>
